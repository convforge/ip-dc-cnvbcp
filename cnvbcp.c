#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>

#include <sybfront.h>
#include <sybdb.h>

RETCODE bcp_delcol(DBPROCESS * dbproc, int table_column);
int bcp_findcol_by_name(DBPROCESS * dbproc, char *column_name);
int bcp_numcols(DBPROCESS * dbproc);

#include "progressbar.h"
#include "DOParser.h"
#include "cnvdate.h"

#define BUFFERNO aliaslen


// defines

#define CNVBCP_SUCCESS 0
#define CNVBCP_FAILURE 1

#define CNVBCP_FALSE 0
#define CNVBCP_TRUE  1

#define CNVBCP_UNKNOWN  0
#define CNVBCP_DSN      1
#define CNVBCP_DATABASE 2
#define CNVBCP_UID      3
#define CNVBCP_PWD      4

// typedefs
typedef struct _cnvbcp_buffer_st
{
   int            column;
   int            type;
   int            dlen;
   int            plen;
   int            length;
   int            malloc_length;
   int            is_nullable;
   int            date;
   int            scale;
   char          *format;
   int            source_type;
   unsigned char *data;
} CNVBCP_BUFFER;

typedef struct _cnvbcp_st
{
   char          *conn;
   char          *dsn;
   char          *user;
   char          *pass;
   char          *server;
   char          *database;
   char          *schema;
   char          *table;
   char          *dbobject;
   DBPROCESS     *dbproc;
   char          *outfile;
   DOPARSER      *p;
   char          *datfile;
   FILE          *datfp;
   char          *badfile;
   FILE          *badfp;
   char          *logfile;
   FILE          *logfp;
   size_t         arraysize;
   size_t         packetsize;
   size_t         firstrow;
   size_t         lastrow;
   char           truncate;
   char           progressbar;
   char           verbose;
   char           reformat_dates;
   char           needs_bcpkeepidentity;
   char           ignore_missing_outfile_fields;
   size_t         nbad;
   size_t         maxbad;
   unsigned char *badrow;
   size_t         rowsize;
   size_t         nrows;
   size_t         ncolumns;
   DBCOL2        *column;
   size_t         nbuffers;
   CNVBCP_BUFFER *buffer;
} CNVBCP;


FILE *LOGFP = NULL;

// prototypes

void print_usage(char *progname);
int cnvbcp_parse_credentials(CNVBCP *cnvbcp);
int cnvbcp_parse_outfile    (CNVBCP *cnvbcp);
int cnvbcp_open_datfile     (CNVBCP *cnvbcp);
int cnvbcp_connect          (CNVBCP *cnvbcp);
int cnvbcp_init_bcp         (CNVBCP *cnvbcp);
int cnvbcp_get_table_info   (CNVBCP *cnvbcp);
int cnvbcp_create_buffers   (CNVBCP *cnvbcp);
int cnvbcp_truncate_table   (CNVBCP *cnvbcp);
int cnvbcp_load_data        (CNVBCP *cnvbcp);
int cnvbcp_bind_nonloading_columns(CNVBCP *cnvbcp);

int cnvbcp_err_handler(DBPROCESS *dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr);
int cnvbcp_msg_handler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line);

char *advance_param(char *s);
int   param_type(char *s);
char *dup_val(char *s);

char *cnvbcp_get_odbc_ini(CNVBCP *cnvbcp);
char *cnvbcp_get_database(CNVBCP *cnvbcp);

int   cnvbcp_remove_unused_columns(CNVBCP *cnvbcp);
void  cnvbcp_dump_buf(const char *buf, size_t length);
int   cnvbcp_set_options(CNVBCP *cnvbcp);
int   cnvbcp_set_bcpkeepidentiy(CNVBCP *cnvbcp);

void wipearg(char *arg);



int
main(int argc, char *argv[])
{
   int          ret = CNVBCP_SUCCESS;
   extern char *optarg;
   extern int   optind;
   int          c;
   char         tmpstr[4096];
   CNVBCP       cnvbcp;

   memset(&cnvbcp, 0, sizeof(cnvbcp));
   cnvbcp.logfp   = stderr;
   cnvbcp.maxbad  = 20;
   cnvbcp.lastrow = (size_t)-1;;

   while((c = getopt(argc, argv, "a:B:b:c:Dd:F:IL:l:o:Pp:s:t:Vx")) != EOF)
   {
      switch(c)
      {
         case 'a': cnvbcp.arraysize                     = atoi(optarg);   break; // insert batch size
         case 'B': cnvbcp.maxbad                        = atoi(optarg);   break; // number of bad records to allow before exit
         case 'b': cnvbcp.badfile                       = strdup(optarg); break; // bad record file name
         case 'c': cnvbcp.conn                          = strdup(optarg); wipearg(optarg); break; // connect string
         case 'D': cnvbcp.reformat_dates                = CNVBCP_TRUE;    break; // reformat dates for compatibility
         case 'd': cnvbcp.datfile                       = strdup(optarg); break; // dat file name
         case 'F': cnvbcp.firstrow                      = atoi(optarg);   break; // first row in file to send
         case 'I': cnvbcp.ignore_missing_outfile_fields = CNVBCP_TRUE;    break; // reformat dates for compatibility
         case 'L': cnvbcp.lastrow                       = atoi(optarg);   break; // last row in file to send
         case 'l': cnvbcp.logfile                       = strdup(optarg); break; // log file name
         case 'o': cnvbcp.outfile                       = strdup(optarg); break; // out file name
         case 'P': cnvbcp.progressbar                   = CNVBCP_TRUE; cnvbcp.verbose = CNVBCP_FALSE; break; // show progress bar
         case 'p': cnvbcp.packetsize                    = atoi(optarg);   break; // data transmission packet size
         case 's': cnvbcp.schema                        = strdup(optarg); break; // schema name
         case 't': cnvbcp.table                         = strdup(optarg); break; // table name
         case 'V': cnvbcp.verbose                       = CNVBCP_TRUE; cnvbcp.progressbar = CNVBCP_FALSE; break; // bcp style progress
         case 'x': cnvbcp.truncate                      = CNVBCP_TRUE;    break; // truncate table before load
      }
   }

   // set reasonable defaults
   if(cnvbcp.arraysize <= 0)
      cnvbcp.arraysize = 1000;

   if(cnvbcp.packetsize <= 0)
      cnvbcp.packetsize = 8192;

   if((argc == 1)               ||
      ((argc - optind)  > 0)    ||
      (cnvbcp.table    == NULL) ||
      (cnvbcp.datfile  == NULL) ||
      (cnvbcp.outfile  == NULL) ||
      (cnvbcp.firstrow  < 0)    ||
      (cnvbcp.lastrow   < 0)    ||
      (cnvbcp.lastrow   < cnvbcp.firstrow))
   {
      print_usage(argv[0]);
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS)
   {
      if(cnvbcp.logfile != NULL)
      {
         if((cnvbcp.logfp = fopen(cnvbcp.logfile, "w")) == NULL)
         {
            cnvbcp.logfp = stderr;
            fprintf(cnvbcp.logfp, "Error opening logfile '%s'.\n", cnvbcp.logfile);
         }
      }
      else
      {
         cnvbcp.logfp = stderr;
      }

      // set file pointer for error and message handlers
      LOGFP = cnvbcp.logfp;
   }

   // don't do anything without credentials
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_parse_credentials(&cnvbcp);

   if(ret == CNVBCP_SUCCESS)
   {
      // for conversion, the freetda.conf block has the same name as the ODBC DSN
      cnvbcp.server = cnvbcp.dsn;

      // assemble dbobject string
      tmpstr[0] = '\0';
      if(cnvbcp.schema != NULL)
      {
         strcat(tmpstr, cnvbcp.schema);
         strcat(tmpstr, ".");
      }
      strcat(tmpstr, cnvbcp.table);
      cnvbcp.dbobject = strdup(tmpstr);
   }

   // parse out file
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_parse_outfile(&cnvbcp);

   // open and check dat file
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_open_datfile(&cnvbcp);

   // check and correct firstrow and lastrow
   if(ret == CNVBCP_SUCCESS)
   {
      if(cnvbcp.firstrow == 0)
         cnvbcp.firstrow = 1;
      if(cnvbcp.lastrow == 0 || cnvbcp.lastrow > cnvbcp.nrows)
         cnvbcp.lastrow = cnvbcp.nrows;
   }

   // connect to the database
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_connect(&cnvbcp);

   // set options
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_set_options(&cnvbcp);

   // initialize bcp module
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_init_bcp(&cnvbcp);

   // remove columns not being processed
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_remove_unused_columns(&cnvbcp);

   // get table description from database
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_get_table_info(&cnvbcp);

   // set BCPKEEPIDENTITY if an identity column in the table
   // is being loaded from the data file
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_set_bcpkeepidentiy(&cnvbcp);

   // cross reference outfile to database
   // and create buffers to bind to transaction
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_create_buffers(&cnvbcp);

#ifdef YET
   // set up default bindings for columns that will not be loaded from this file
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_bind_nonloading_columns(&cnvbcp);
#endif

   // everything is set up, out file is parsed, dat file is opened and checked,
   // table is described and data buffers are allocated.
   // last step before loading data is to truncate the table if requested
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_truncate_table(&cnvbcp);

   // do the deed
   if(ret == CNVBCP_SUCCESS)
      ret = cnvbcp_load_data(&cnvbcp);

   return(ret);
}



void
print_usage(char *progname)
{
   printf("\nUsage:\n");
   printf("%s -t table -d datfile -o outfile [options...]\n", progname);
   printf("   -a arraysize  - The number of rows to insert between checkpoints. (default 1000)\n");
   printf("   -B maxbad     - The number of bad inserts allowed before exit. (default 20)\n");
   printf("   -b badfile    - The name of the file to write rejected records to.\n");
   printf("   -c connstring - The database connect string.\n");
   printf("   -D            - Reformat date strings for database compatibility.\n");
   printf("   -d datfile    - The name of the file containing the table data.\n");
   printf("   -F firstrow   - The first row of file to send.\n");
   printf("   -I            - Ignore outfile fields not in the table columns.\n");
   printf("   -L lastrow    - The last row of file to send.\n");
   printf("   -l logfile    - The name of the file to write log messages to.\n");
   printf("   -o outfile    - The name of the file describing the data file layout.\n");
   printf("   -P            - Produce progress bar output.\n");
   printf("   -p packetsize - The data transmission packet size.\n");
   printf("   -s schema     - The schema in which the table exists.\n");
   printf("   -t table      - The name of the table to load.\n");
   printf("   -V            - Produce text progress output.\n");
   printf("   -x            - Truncate the table before loading.\n");
   printf("\n");
   printf("Notes:\n");
   printf("   This program uses the environment variable CNVBCP_PSWD to connect to the\n");
   printf("   database.  If CNVBCP_PSWD is not set and a connect string is not specified\n");
   printf("   on the command line (discouraged) you will be prompted for a connect string.\n");
   printf("\n");
   printf("   A connect string must contain at least \"DSN=[dsn];UID=[uid];PWD=[pwd];\"\n");
   printf("\n");
}



int
cnvbcp_parse_credentials(CNVBCP *cnvbcp)
{
   int ret = CNVBCP_SUCCESS;
   char connectstring[1024];
   char *param;

   // if a connect string was not given on the command line look in CNVBCP_PSWD
   if(cnvbcp->conn == NULL)
      cnvbcp->conn = (char *)getenv("CNVBCP_PSWD");

   if(cnvbcp->conn == NULL) // if not on command line and not in ENV
   {
      // Prompt for the password
      printf("Enter the database connect string: ");

      fgets(connectstring, sizeof(connectstring), stdin);
      connectstring[strlen(connectstring)-1] = '\0'; //NULL;

      cnvbcp->conn = strdup(connectstring);
   }

   // parse the connection info as DSN=[dsn];UID=[uid];PWD=[pwd]; in any order
   connectstring[0] = ';';
   strcpy(&connectstring[1], cnvbcp->conn);

   param = connectstring;
   while((param = advance_param(param)) != NULL)
   {
      switch(param_type(param))
      {
         case CNVBCP_DSN:       cnvbcp->dsn      = dup_val(param); break;
         case CNVBCP_DATABASE:  cnvbcp->database = dup_val(param); break;
         case CNVBCP_UID:       cnvbcp->user     = dup_val(param); break;
         case CNVBCP_PWD:       cnvbcp->pass     = dup_val(param); break;
         default: break;
      }
   }

   // if the database wasn't in the connect string get it the hard way
   if(cnvbcp->database == NULL)
      cnvbcp->database = cnvbcp_get_database(cnvbcp);

   // sanity check credentials (only goes so far)
   if(cnvbcp->dsn == NULL || cnvbcp->database == NULL || cnvbcp->user == NULL || cnvbcp->pass == NULL)
   {
      fprintf(cnvbcp->logfp, "incomplete credentials.  Check connect string and odbc.ini file.\n");
      ret = CNVBCP_FAILURE;
   }

   return(ret);
}



int
cnvbcp_parse_outfile(CNVBCP *cnvbcp)
{
   int ret = CNVBCP_SUCCESS;

   // parse the dot out file
   if((cnvbcp->p = dop_init(cnvbcp->outfile, 0, stdout)) != NULL)
   {
      // actually do the parse
      if(dop_parse(cnvbcp->p) != DOP_SUCCESS)
      {
         dop_free(cnvbcp->p);
         ret = CNVBCP_FAILURE;
      }
   }
   else
   {
      ret = CNVBCP_FAILURE;
   }

   return(ret);
}



int
cnvbcp_open_datfile(CNVBCP *cnvbcp)
{
   int         ret = CNVBCP_SUCCESS;
   size_t      filesize, numrecs, reclen;
   struct stat statbuf;

   if((cnvbcp->datfp = fopen(cnvbcp->datfile, "r")) == NULL)
   {
      fprintf(cnvbcp->logfp, "Couldn't open '%s' for input.\n", cnvbcp->datfile);
      fprintf(cnvbcp->logfp, "open: %s\n", strerror(errno));
      ret = CNVBCP_FAILURE;
   }

   // Determine the data file size
   if(ret == CNVBCP_SUCCESS &&
      fstat(fileno(cnvbcp->datfp), &statbuf) != 0)
   {
      fprintf(cnvbcp->logfp, "%s could not be fstat'd.\n", cnvbcp->datfile);
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS &&
      statbuf.st_size == 0)
   {
      fprintf(cnvbcp->logfp, "%s is empty.\n", cnvbcp->datfile);
      ret = CNVBCP_FAILURE;
   }

   reclen   = cnvbcp->p->reclen;
   filesize = statbuf.st_size;
   numrecs  = filesize / reclen;

   if(ret == CNVBCP_SUCCESS &&
      (numrecs * reclen) != filesize)
   {
      fprintf(cnvbcp->logfp, "%s contains fractional rows.\n", cnvbcp->datfile);
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS)
   {
      cnvbcp->nrows   = numrecs;
      cnvbcp->rowsize = reclen;
   }

   return(ret);
}



int
cnvbcp_connect(CNVBCP *cnvbcp)
{
   int       ret = CNVBCP_SUCCESS;
   LOGINREC *login = NULL;

   // Initialize DB-Library
   if(dbinit() == FAIL)
   {
      ret = CNVBCP_FAILURE;
   }

   // Install the user-supplied error-handling and message-handling
   // routines. They are defined at the bottom of this source file.
   if(ret == CNVBCP_SUCCESS)
   {
      dberrhandle(cnvbcp_err_handler);
      dbmsghandle(cnvbcp_msg_handler);
   }

   // Allocate and initialize the LOGINREC structure to be used
   // to open a connection to SQL Server.
   if(ret == CNVBCP_SUCCESS &&
      (login = dblogin()) == NULL)
   {
      ret = CNVBCP_FAILURE;
   }

   // Set application name
   DBSETLAPP(login, "CNVBCP");

   // set network packet size
   if(cnvbcp->packetsize > 0)
   {
      DBSETLPACKET(login, cnvbcp->packetsize);
   }

   // Enable bulk copy
   BCP_SETL(login, TRUE);

   // set credentials
   DBSETLUSER(login,   cnvbcp->user);
   DBSETLPWD(login,    cnvbcp->pass);
   DBSETLDBNAME(login, cnvbcp->database);

   // Get a connection to the database.
   if((cnvbcp->dbproc = dbopen(login, cnvbcp->server)) == NULL)
   {
      fprintf(stderr, "Can't connect to server \"%s\".\n", cnvbcp->server);
      dbloginfree(login);
      ret = CNVBCP_FAILURE;
   }

   // clean up
   dbloginfree(login);
   login = NULL;

   return(ret);
}



int
cnvbcp_set_options(CNVBCP *cnvbcp)
{
   int ret = CNVBCP_SUCCESS;
   RETCODE fOK;

   if(dbcmd(cnvbcp->dbproc, "set quoted_identifier on;") == FAIL)
   {
      fprintf(stderr, "setoptions() failed preparing quoted_identifier\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS &&
      dbcmd(cnvbcp->dbproc, "set ansi_warnings on;") == FAIL)
   {
      fprintf(stderr, "setoptions() failed preparing ansi_warnings\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS &&
      dbcmd(cnvbcp->dbproc, "set ansi_padding on;") == FAIL)
   {
      fprintf(stderr, "setoptions() failed preparing ansi_padding\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS &&
      dbcmd(cnvbcp->dbproc, "set ansi_nulls on;") == FAIL)
   {
      fprintf(stderr, "setoptions() failed preparing ansi_nulls\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS &&
      dbcmd(cnvbcp->dbproc, "set concat_null_yields_null on;") == FAIL)
   {
      fprintf(stderr, "setoptions() failed preparing concat_null_yields_null\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS &&
      dbsqlexec(cnvbcp->dbproc) == FAIL)
   {
      fprintf(stderr, "setoptions() failed sending options\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS)
   {
      while((fOK = dbresults(cnvbcp->dbproc)) == SUCCEED)
      {
         while((fOK = dbnextrow(cnvbcp->dbproc)) == REG_ROW)
         {
            continue;
         }
         if(fOK == FAIL)
         {
            fprintf(stderr, "setoptions() failed getting rows\n");
            ret = CNVBCP_FAILURE;
         }
      }
      if(fOK == FAIL)
      {
         fprintf(stderr, "setoptions() failed getting results\n");
         ret = CNVBCP_FAILURE;
      }
   }

   return(ret);
}



int
cnvbcp_init_bcp(CNVBCP *cnvbcp)
{
   int ret = CNVBCP_SUCCESS;

   if(bcp_init(cnvbcp->dbproc, cnvbcp->dbobject, NULL, NULL, DB_IN) == FAIL)
   {
      fprintf(cnvbcp->logfp, "bcp_init error.\n");
      ret = CNVBCP_FAILURE;
   }

   return(ret);
}



int
cnvbcp_remove_unused_columns(CNVBCP *cnvbcp)
{
   int    ret = CNVBCP_SUCCESS;
   int    c, d, f;
   DBCOL2 column;
   int    ncolumns;

   if((ncolumns = dbnumcols(cnvbcp->dbproc)) == 0)
   {
      fprintf(cnvbcp->logfp, "Error in ncolumns\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS)
   {
      for(c=1; ret == CNVBCP_SUCCESS && c<=ncolumns; c++)
      {
         // printf("c = %d, ncolumns = %d\n", c, ncolumns);
         column.SizeOfStruct = sizeof(DBCOL2);
         if(dbtablecolinfo(cnvbcp->dbproc, c, (DBCOL *)&column) == FAIL)
         {
            fprintf(cnvbcp->logfp, "dbtablecolinfo[%d] failed.\n", c);
            ret = CNVBCP_FAILURE;
            break;
         }
         // printf("looking for %s\n", column.Name);

         if(ret == CNVBCP_SUCCESS)
         {
            // look for the field in the outfile that matches this column
            for(f=0; f<cnvbcp->p->nfield; f++)
            {
               if(strcasecmp(cnvbcp->p->field[f].name, column.Name) == 0)
               {
                  // column found, fall out
                  break;
               }
            }
            if(f == cnvbcp->p->nfield)
            {
               // column not found, remove it
               if((d = bcp_findcol_by_name(cnvbcp->dbproc, column.Name)) != -1)
               {
                  // printf("Removing %s\n", column.Name);
                  if(bcp_delcol(cnvbcp->dbproc, d) == FAIL)
                  {
                     fprintf(cnvbcp->logfp, "bcp_delcol[%d] failed.\n", d);
                     ret = CNVBCP_FAILURE;
                  }
               }
            }
         }
      }
   }

   return(ret);
}



int
cnvbcp_get_table_info(CNVBCP *cnvbcp)
{
   int    ret = CNVBCP_SUCCESS;
   int    c, i, ncolumns_actual;
   DBCOL2 column;

   if((ncolumns_actual = dbnumcols(cnvbcp->dbproc)) == 0)
   {
      fprintf(cnvbcp->logfp, "Error in ncolumns\n");
      ret = CNVBCP_FAILURE;
   }

   if((cnvbcp->ncolumns = bcp_numcols(cnvbcp->dbproc)) == 0)
   {
      fprintf(cnvbcp->logfp, "Error in ncolumns\n");
      ret = CNVBCP_FAILURE;
   }

   if((cnvbcp->column = (DBCOL2 *)calloc(cnvbcp->ncolumns+1, sizeof(DBCOL2))) == NULL)
   {
      fprintf(cnvbcp->logfp, "table calloc failed.\n");
      ret = CNVBCP_FAILURE;
   }

   if(ret == CNVBCP_SUCCESS)
   {
      // printf("Allocated %zd columns\n", cnvbcp->ncolumns+1);

      for(i=1; i<=ncolumns_actual; i++)
      {
         column.SizeOfStruct = sizeof(DBCOL2);
         if(dbtablecolinfo(cnvbcp->dbproc, i, (DBCOL *)&column) == FAIL)
         {
            fprintf(cnvbcp->logfp, "dbtablecolinfo[%d] failed.\n", i);
            ret = CNVBCP_FAILURE;
            break;
         }
         if((c = bcp_findcol_by_name(cnvbcp->dbproc, column.Name)) != -1)
         {
            cnvbcp->column[c].SizeOfStruct = sizeof(DBCOL2);
            if(dbtablecolinfo(cnvbcp->dbproc, i, (DBCOL *)&cnvbcp->column[c]) == FAIL)
            {
               fprintf(cnvbcp->logfp, "dbtablecolinfo[%d] failed.\n", c);
               ret = CNVBCP_FAILURE;
               break;
            }
            if(cnvbcp->column[c].Identity == CNVBCP_TRUE)
            {
               cnvbcp->needs_bcpkeepidentity = CNVBCP_TRUE;
            }
         }
      }
   }

   return(ret);
}



int
cnvbcp_set_bcpkeepidentiy(CNVBCP *cnvbcp)
{
   int ret = CNVBCP_SUCCESS;

   if(cnvbcp->needs_bcpkeepidentity == TRUE)
   {
      bcp_control(cnvbcp->dbproc, BCPKEEPIDENTITY, 1);

      if(dbfcmd(cnvbcp->dbproc, "set identity_insert %s on", cnvbcp->dbobject) == FAIL)
      {
         fprintf(stderr, "dbfcmd failed\n");
         ret = CNVBCP_FAILURE;
      }

      if(ret == CNVBCP_SUCCESS &&
         dbsqlexec(cnvbcp->dbproc) == FAIL)
      {
         fprintf(stderr, "dbsqlexec failed\n");
         ret = CNVBCP_FAILURE;
      }

      if(ret == CNVBCP_SUCCESS)
      {
         while (NO_MORE_RESULTS != dbresults(cnvbcp->dbproc))
            continue;
      }
   }

   return(ret);
}



int
cnvbcp_create_buffers(CNVBCP *cnvbcp)
{
   int ret = CNVBCP_SUCCESS;
   int b, f, c;

   // make things easier to find later
   cnvbcp->nbuffers = cnvbcp->p->nfield;

   // re-use the line attribute of the dop_field structure to store the table column number
   // first set them all to -1 for error checking along the way
   for(f=0; f<cnvbcp->p->nfield; f++)
   {
      cnvbcp->p->field[f].line = -1;
   }

   // check to see if all fields in the out file are in the table
   for(f=0; f<cnvbcp->p->nfield; f++)
   {
      for(c=1; ret == CNVBCP_SUCCESS && c<=cnvbcp->ncolumns; c++)
      {
         if(strcasecmp(cnvbcp->p->field[f].name, cnvbcp->column[c].Name) == 0)
         {
            if(cnvbcp->p->field[f].line == -1)
            {
               cnvbcp->p->field[f].line = c;
            }
            else
            {
               fprintf(cnvbcp->logfp, "%s found twice.\n", cnvbcp->p->field[f].name);
               ret = CNVBCP_FAILURE;
            }
            break;
         }
      }
      if(c > cnvbcp->ncolumns)
      {
         fprintf(cnvbcp->logfp, "%s not found in table.\n", cnvbcp->p->field[f].name);
         if(cnvbcp->ignore_missing_outfile_fields == CNVBCP_TRUE)
         {
            // leaving -1 in the line mark marks this field as not being used 
            // just reduce the number of buffers to allocate
            cnvbcp->nbuffers--;
         }
         else
         {
            ret = CNVBCP_FAILURE;
         }
      }
   }

   // check to make sure there's at least one field to load
   if(cnvbcp->nbuffers <= 0)
   {
      fprintf(cnvbcp->logfp, "No tables columns are found in the out file field list.\n");
      ret = CNVBCP_FAILURE;
   }

   // allocate an array of buffer structures for the columns in the out file
   if(ret == CNVBCP_SUCCESS)
   {
      if((cnvbcp->buffer = (CNVBCP_BUFFER *)calloc(cnvbcp->nbuffers, sizeof(CNVBCP_BUFFER))) == NULL)
      {
         fprintf(cnvbcp->logfp, "buffer calloc failed.\n");
         ret = CNVBCP_FAILURE;
      }
   }


   if(ret == CNVBCP_SUCCESS)
   {
      // loop over all fields in the out file
      for(f=0, b=-1; ret == CNVBCP_SUCCESS && f<cnvbcp->p->nfield; f++)
      {
         // skip this field if line is left as -1 since this indicates it's not in the table
         if(cnvbcp->p->field[f].line == -1)
            continue;

         // advance to the next available buffer
         b++;

         // set table column number
         cnvbcp->buffer[b].column = cnvbcp->p->field[f].line;

         // also set the buffer number
         cnvbcp->p->field[f].BUFFERNO = b;

         // set length
         cnvbcp->buffer[b].length = cnvbcp->p->field[f].length;
         cnvbcp->buffer[b].malloc_length = cnvbcp->p->field[f].length + 2;

         cnvbcp->buffer[b].source_type = cnvbcp->p->field[f].type;

         // set column type
         switch(cnvbcp->p->field[f].type)
         {
            case DOP_CHAR:
            case DOP_CLOB:
            case DOP_RAW:
               cnvbcp->buffer[b].type = SYBCHAR;
               cnvbcp->buffer[b].plen = 0;
               cnvbcp->buffer[b].dlen = cnvbcp->buffer[b].length;
               break;
            case DOP_VARCHAR:
               cnvbcp->buffer[b].type = SYBCHAR;
               cnvbcp->buffer[b].plen = 2;
               cnvbcp->buffer[b].dlen = cnvbcp->buffer[b].length - 2;
               cnvbcp->buffer[b].dlen = cnvbcp->buffer[b].length;
               break;
            case DOP_NCHAR:
               cnvbcp->buffer[b].type = SYBCHAR;
               cnvbcp->buffer[b].plen = 0;
               cnvbcp->buffer[b].dlen = cnvbcp->buffer[b].length * 2;
               break;
            case DOP_NVARCHAR:
               cnvbcp->buffer[b].type = SYBCHAR;
               cnvbcp->buffer[b].plen = 2;
               cnvbcp->buffer[b].dlen = cnvbcp->buffer[b].length * 2;
               break;
            case DOP_DATE:
            case DOP_DATETIME:
            case DOP_TIMESTAMP:
               cnvbcp->buffer[b].malloc_length = 28;
               cnvbcp->buffer[b].type = SYBCHAR;
               cnvbcp->buffer[b].plen = 0;
               cnvbcp->buffer[b].dlen = 26;
               if(cnvbcp->reformat_dates == CNVBCP_TRUE)
               {
                  cnvbcp->buffer[b].dlen = 28;
                  cnvbcp->buffer[b].date = CNVBCP_TRUE;
                  if(strcmp(cnvbcp->p->field[f].format, "YYYYMMDDHH24MISS") == 0)
                     cnvbcp->buffer[b].format = strdup("%C%y%m%d%H%M%S");
                  else
                     cnvbcp->buffer[b].format = strdup(cnvbcp->p->field[f].format);
               }
               break;
            case DOP_BIGINT:
            case DOP_BIT:
            case DOP_INTEGER:
            case DOP_SMALLINT:
            case DOP_TINYINT:
               switch(cnvbcp->p->field[f].length)
               {
                  case 1:
                     cnvbcp->buffer[b].type = SYBINT1;
                     break;
                  case 2:
                     cnvbcp->buffer[b].type = SYBINT2;
                     break;
                  case 4:
                     cnvbcp->buffer[b].type = SYBINT4;
                     break;
                  case 8:
                     cnvbcp->buffer[b].type = SYBINT8;
                     break;
               }
               cnvbcp->buffer[b].plen = 0;
               cnvbcp->buffer[b].dlen = -1;
               break;
            case DOP_DOUBLE:
               if(cnvbcp->p->field[f].has_scale == DOP_TRUE)
               {
                  // use scale information to sprintf the value to a character string
                  // before sending to the db
                  cnvbcp->buffer[b].type  = SYBCHAR;
                  cnvbcp->buffer[b].plen  = 0;
                  cnvbcp->buffer[b].dlen  = 128;
                  cnvbcp->buffer[b].scale = cnvbcp->p->field[f].scale;
               }
               else
               {
                  cnvbcp->buffer[b].type = SYBFLT8;
                  cnvbcp->buffer[b].plen = 0;
                  cnvbcp->buffer[b].dlen = -1;
               }
               break;
         }

         // set nullability
         cnvbcp->buffer[b].is_nullable = cnvbcp->p->field[f].is_nullable;

         // allocate malloc length + 2 bytes of space for the prefix and field data
         if((cnvbcp->buffer[b].data = (unsigned char *)malloc(cnvbcp->buffer[b].malloc_length)) == NULL)
         {
            fprintf(cnvbcp->logfp, "field %d calloc failed.\n", f);
            ret = CNVBCP_FAILURE;
         }
      }
   }

   // allocate space to transfer rejected rows to the badfile and read unused fields
   if(ret == CNVBCP_SUCCESS)
   {
      if((cnvbcp->badrow = (unsigned char *)malloc(cnvbcp->rowsize)) == NULL)
      {
         fprintf(cnvbcp->logfp, "badrow malloc failed.\n");
         ret = CNVBCP_FAILURE;
      }
   }

   return(ret);
}



#ifdef YET
int
cnvbcp_bind_nonloading_columns(CNVBCP *cnvbcp)
{
   int    ret = CNVBCP_SUCCESS;
   size_t b, c;
   int dlen;

   // call bcp_bind() for each column that will not be loaded
   for(c=1; ret == CNVBCP_SUCCESS && c<=cnvbcp->ncolumns; c++)
   {
      for(b=0; ret == CNVBCP_SUCCESS && b<cnvbcp->nbuffers; b++)
      {
         if(cnvbcp->buffer[b].column == c)
         {
            // this column doesn't need a default bind
            break;
         }
      }

      if(b == cnvbcp->nbuffers)
      {
         // a pre-allocated buffer wasn't found for this column
         // so set a default empty/NULL binding
         // assume empty
         dlen = -1;
         if(cnvbcp->column[c].Null == 1)
         {
            // modify to NULL
            dlen = 0;
         }

         if(bcp_bind(cnvbcp->dbproc, // dbproc       database connection info
                     (BYTE *)"",        // varaddr      address of host variable
                     0,                 // prefixlen    size of length prefix at the beginning of varaddr, 0 for fixedlength datatypes.
                     dlen,              // varlen       number of bytes in varaddr.  Zero for NULL, -1 for fixed-length datatypes.
                     NULL,              // terminator   byte sequence that marks the end of the data in varaddr
                     0,                 // termlen      length of terminator
                     SYBCHAR,           // vartype      datatype of the host variable
                     c)                 // table_column column nuber, starting at 1, in the table.
            == FAIL)
         {
            fprintf(cnvbcp->logfp, "Error binding unused column %ld.\n", c);
            ret = CNVBCP_FAILURE;
         }
      }
   }

   return(ret);
}
#endif



int
cnvbcp_truncate_table(CNVBCP *cnvbcp)
{
   int ret = CNVBCP_SUCCESS;
   char      statement[256];

   CNVBCP c;  // local context for second session so as not to disturb
   // the BCP mode already set up in the existing session

   if(cnvbcp->truncate == CNVBCP_TRUE)
   {
      memset(&c, 0, sizeof(c));

      // transfer essential information to the local connection
      c.dsn      = cnvbcp->dsn;
      c.user     = cnvbcp->user;
      c.pass     = cnvbcp->pass;
      c.server   = cnvbcp->server;
      c.database = cnvbcp->database;
      c.schema   = cnvbcp->schema;
      c.table    = cnvbcp->table;
      c.dbobject = cnvbcp->dbobject;
      c.dbproc   = cnvbcp->dbproc;
      c.logfp    = cnvbcp->logfp;

      fprintf(c.logfp, "Truncating %s\n", c.dbobject);

      // prepare the truncate statement
      sprintf(statement, "truncate table %s", c.dbobject);

      // connect to the database
      if(ret == CNVBCP_SUCCESS)
         ret = cnvbcp_connect(&c);

      // send the truncate statement to the database
      if(ret == CNVBCP_SUCCESS &&
         dbcmd(c.dbproc, statement) == FAIL)
      {
         fprintf(c.logfp, "dbcmd for truncate statement failed\n");
         ret = CNVBCP_FAILURE;
      }

      // execute the truncate statement
      if(ret == CNVBCP_SUCCESS &&
         dbsqlexec(c.dbproc) == FAIL)
      {
         fprintf(c.logfp, "dbsqlexec truncate statement failed\n");
         ret = CNVBCP_FAILURE;
      }

      // receive results
      if(ret == CNVBCP_SUCCESS &&
         dbresults(c.dbproc) == FAIL)
      {
         fprintf(c.logfp, "Error in dbresults for truncate statement\n");
         ret = CNVBCP_FAILURE;
      }

      dbclose(c.dbproc);
   }

   return(ret);
}



// The order of operation is:
//    call bcp_bind() to set the buffers to read from
//    loop
//      read data from the input file into pre-allocated buffers
//      set NULL or length (bcp_collen)
//      reformat the date column if necessary
//      call bcp_sendrow to send the data to the DB
//      if num_rows-sent % batch_size == 0
//        call bcp_batch() to commit a set of rows
//    endloop
//    call bcp_done() to commit any final records and close the session
int
cnvbcp_load_data(CNVBCP *cnvbcp)
{
   int                ret = CNVBCP_SUCCESS;
   int                dlen, nullindicator;
   int                batchcount = 0;
   int                length_already_set = CNVBCP_FALSE;
   size_t             b, f, r;
   CNVBCP_BUFFER     *buf = NULL;
   struct _cnv_dtetm  tm;

   // call bcp_bind() for each column that will be loaded
   for(b=0; ret == CNVBCP_SUCCESS && b<cnvbcp->nbuffers; b++)
   {
      // make things easier to reference
      buf = &cnvbcp->buffer[b];

      if(bcp_bind(cnvbcp->dbproc, // dbproc       database connection info
                  buf->data,      // varaddr      address of host variable
                  buf->plen,      // prefixlen    size of length prefix at the beginning of varaddr, 0 for fixed-length datatypes.
                  buf->dlen,      // varlen       number of bytes of data in varaddr.  Zero for NULL, -1 for fixed-length datatypes.
                  NULL,           // terminator   byte sequence that marks the end of the data in varaddr
                  0,              // termlen      length of terminator
                  buf->type,      // vartype      datatype of the host variable
                  buf->column)    // table_column column nuber, starting at 1, in the table.
         == FAIL)
      {
         fprintf(cnvbcp->logfp, "Error binding column %ld.\n", b);
         ret = CNVBCP_FAILURE;
      }
   }

   // seek to the location of the first row to send
   if(cnvbcp->firstrow > 1)
   {
      if(fseek(cnvbcp->datfp, ((cnvbcp->firstrow-1)*cnvbcp->rowsize), SEEK_SET) != 0)
      {
         fprintf(cnvbcp->logfp, "Error advancing to the first row in the file to send.\n");
         ret = CNVBCP_FAILURE;
      }
   }

   // initialize the progress bar
   if(cnvbcp->progressbar == CNVBCP_TRUE)
   {
      char header[128];
      sprintf(header, "Sending %ld rows...", (cnvbcp->lastrow - cnvbcp->firstrow + 1));
      pb_init_bar(cnvbcp->logfp, header, (cnvbcp->lastrow - cnvbcp->firstrow + 1), PB_REPORTTIME);
   }

   // loop over all rows to be sent from the data file
   for(r=cnvbcp->firstrow; ret == CNVBCP_SUCCESS && r<=cnvbcp->lastrow; r++)
   {
      // read a set of fields from the input file
      for(f=0; ret == CNVBCP_SUCCESS && f<cnvbcp->p->nfield; f++)
      {
         length_already_set = CNVBCP_FALSE;

         // make things easier to reference
         if(cnvbcp->p->field[f].line == -1)
         {
            // this field isn't being sent to the DB but it must still be read from the file
            if(cnvbcp->p->field[f].is_nullable)
            {
               if((nullindicator = fgetc(cnvbcp->datfp)) == EOF)
               {
                  fprintf(cnvbcp->logfp, "Error reading null indicator at row %ld unused field %ld.\n", r, f);
                  ret = CNVBCP_FAILURE;
               }
            }
            // make temporary use of the bad buffer to read this unused field
            if(fread(cnvbcp->badrow, 1, cnvbcp->p->field[f].length,  cnvbcp->datfp) != cnvbcp->p->field[f].length)
            {
               fprintf(cnvbcp->logfp, "Error reading data at row %ld unused field %ld.\n", r, f);
               ret = CNVBCP_FAILURE;
            }
            continue;
         }
         
         // get the right buffer for this field
         buf = &cnvbcp->buffer[cnvbcp->p->field[f].BUFFERNO];

         // read null indicator
         nullindicator = 'N';
         if(buf->is_nullable)
         {
            if((nullindicator = fgetc(cnvbcp->datfp)) == EOF)
            {
               fprintf(cnvbcp->logfp, "Error reading null indicator at row %ld field %ld.\n", r, f);
               ret = CNVBCP_FAILURE;
            }
         }

         // read the data
         if(ret == CNVBCP_SUCCESS)
         {
            if(buf->source_type == DOP_DOUBLE && buf->type == SYBCHAR)
            {
               // read a double from the data file and print it to the buffer as a character string
               double tmp_double;
               if(fread(&tmp_double, 1, sizeof(tmp_double), cnvbcp->datfp) != sizeof(tmp_double))
               {
                  fprintf(cnvbcp->logfp, "Error reading data at row %ld field %ld.\n", r, f);
                  ret = CNVBCP_FAILURE;
               }
               else
               {
                  if(buf->is_nullable && nullindicator != 'N')
                  {
                     dlen = 0;
                     buf->data[0] = '\0';
                  }
                  else
                  {
                     // use sprintf to convert the binary to a string getting the new data length
                     // (the number of characters written) into dlen
                     dlen = sprintf((char *)buf->data, "%.*f", buf->scale, tmp_double);
                  }

                  // update the data length for this column
                  if(bcp_collen(cnvbcp->dbproc, dlen, buf->column) == FAIL)
                  {
                     fprintf(cnvbcp->logfp, "Error setting data length at row %ld field %ld.\n", r, f);
                     ret = CNVBCP_FAILURE;
                  }
                  length_already_set = CNVBCP_TRUE;
               }
            }
            else
            {
               if(fread(buf->data, 1, buf->length, cnvbcp->datfp) != buf->length)
               {
                  fprintf(cnvbcp->logfp, "Error reading data at row %ld field %ld.\n", r, f);
                  ret = CNVBCP_FAILURE;
               }
            }
            // printf("Read %s %d bytes [%c] \n", cnvbcp->p->field[b].name, buf->length, buf->is_nullable ? nullindicator : ' ');
            // cnvbcp_dump_buf((char *)buf->data, buf->length);
         }

         // hack the length if the column is nullable
         if(buf->is_nullable && length_already_set == CNVBCP_FALSE)
         {
            dlen = buf->dlen;
            if(nullindicator != 'N')
            {
               dlen = 0;
            }

            if(bcp_collen(cnvbcp->dbproc, dlen, buf->column) == FAIL)
            {
               fprintf(cnvbcp->logfp, "Error setting data length at row %ld field %ld.\n", r, f);
               ret = CNVBCP_FAILURE;
            }
         }

         // reformat date column if necessary
         if(buf->date == CNVBCP_TRUE && nullindicator == 'N' && buf->format != NULL)
         {
            buf->data[buf->length] = '\0';
            dte_strptime((const char *)buf->data, buf->format, &tm);
            dte_strftime((char *)buf->data, buf->malloc_length, "%F %T", &tm);
         }
      }

      // call bcp_sendrow to send the data to the DB
      if(bcp_sendrow(cnvbcp->dbproc) == FAIL)
      {
         // write the rejected record to the bad file if one was requested
         if(cnvbcp->badfile != NULL)
         {
            // open the badfile
            if(cnvbcp->badfp == NULL)
            {
               if((cnvbcp->badfp = fopen(cnvbcp->badfile, "w")) == NULL)
               {
                  fprintf(cnvbcp->logfp, "Error badfile for output. Disabling bad record logging.\n");
                  free(cnvbcp->badfile);
                  cnvbcp->badfile = NULL;
                  goto after_write;
               }
            }

            // go back to the beginning of the rejected record in the data file
            // an error here means the stream is corrupted and further reads
            // can't be trusted
            if(fseek(cnvbcp->datfp, (-1 * cnvbcp->rowsize), SEEK_CUR) != 0)
            {
               fprintf(cnvbcp->logfp, "Error seeking to beginning of rejected record.\n");
               ret = CNVBCP_FAILURE;
            }
            else
            {
               // read the record into the badrow bufer
               // an error here means the stream is corrupted and further reads
               // can't be trusted
               if(fread(cnvbcp->badrow, 1, cnvbcp->rowsize, cnvbcp->datfp) != cnvbcp->rowsize)
               {
                  fprintf(cnvbcp->logfp, "Error reading rejected record.\n");
                  ret = CNVBCP_FAILURE;
               }
               else
               {
                  // write the badrow to the badfile
                  // errors for the steps above but if an error occurs here it
                  // doesn't mean the load to the DB is corrupted
                  if(fwrite(cnvbcp->badrow, 1, cnvbcp->rowsize, cnvbcp->badfp) != cnvbcp->rowsize)
                  {
                     fprintf(cnvbcp->logfp, "Error writing rejected record.\n");
                  }
               }
            }

after_write:
            // empty statement to satisfy goto requirements
            ;
         }

         // do log and max bad record maintenance
         fprintf(cnvbcp->logfp, "Error sending row %ld.\n", r);
         cnvbcp->nbad++;
         if(cnvbcp->nbad >= cnvbcp->maxbad)
         {
            fprintf(cnvbcp->logfp, "Maximum rejected records reached.\n");
            ret = CNVBCP_FAILURE;
         }
      }

      // update the progress bar
      if(cnvbcp->progressbar == CNVBCP_TRUE)
      {
         pb_update_bar();
      }

      // if arraysize > 0  call bcp_batch() to commit a set of rows
      // arraysize gets a nonzero default in main
      if(cnvbcp->arraysize > 0)
      {
         batchcount++;
         if(batchcount == cnvbcp->arraysize)
         {
            batchcount = 0;
            if(bcp_batch(cnvbcp->dbproc) == FAIL)
            {
               fprintf(cnvbcp->logfp, "Error committing batch.\n");
               ret = CNVBCP_FAILURE;
            }
            else
            {
               // print status
               if(cnvbcp->verbose == CNVBCP_TRUE)
               {
                  fprintf(cnvbcp->logfp, "%ld rows sent to database\n", (r - cnvbcp->firstrow + 1));
                  fflush(cnvbcp->logfp);
               }
            }
         }
      }
   }

   // call bcp_done()
   if(batchcount > 0 && bcp_done(cnvbcp->dbproc) == FAIL)
   {
      fprintf(cnvbcp->logfp, "Error finishing load.\n");
   }
   else
   {
      // finalize the progress bar
      if(cnvbcp->progressbar == CNVBCP_TRUE)
      {
         pb_finish_bar();
      }

      // print final message
      fprintf(cnvbcp->logfp, "%ld rows sent to database\n", (r - cnvbcp->firstrow));
   }

   return(ret);
}



int
cnvbcp_err_handler(DBPROCESS * dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr)
{
   static int sent = 0;

   if(dberr == SYBEBBCI)
   {
      // Batch successfully bulk copied to the server
      int batch = bcp_getbatchsize(dbproc);
      fprintf(LOGFP, "%d rows sent to SQL Server.\n", sent += batch);
   }
   else
   {
      if(dberr)
      {
         fprintf(LOGFP, "Msg %d, Level %d\n", dberr, severity);
         fprintf(LOGFP, "%s\n\n", dberrstr);
      }
      else
      {
         fprintf(LOGFP, "DB-LIBRARY error:\n\t");
         fprintf(LOGFP, "%s\n", dberrstr);
      }
   }

   return(INT_CANCEL);
}



int
cnvbcp_msg_handler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line)
{
   // ignore database change and language change messages
   if(msgno != 5701 && msgno != 5703)
   {
      fprintf(LOGFP, "Msg %ld, Level %d, State %d\n", (long) msgno, severity, msgstate);

      if(strlen(srvname) > 0)
         fprintf(LOGFP, "Server '%s', ", srvname);
      if(strlen(procname) > 0)
         fprintf(LOGFP, "Procedure '%s', ", procname);
      if(line > 0)
         fprintf(LOGFP, "Line %d", line);

      fprintf(LOGFP, "\n\t%s\n", msgtext);
   }

   return(0);
}



char *
advance_param(char *s)
{
   char *ret = s;

   // consume characters up to semicolon
   while(*ret != ';' && *ret != '\0')
   {
      if(*ret == '{')
      {
         // skip to closing brace
         while(*ret != '}' && *ret != '\0')
            ret++;
      }
      ret++;
   }

   // advance past the semicolon
   if(*ret == ';')
      ret++;

   if(*ret == '\0')
      ret = NULL;

   return(ret);
}



int
param_type(char *s)
{
   int ret = CNVBCP_UNKNOWN;

   if(strncasecmp(s, "DSN=",      4) == 0) { ret = CNVBCP_DSN;       }
   if(strncasecmp(s, "DATABASE=", 9) == 0) { ret = CNVBCP_DATABASE;  }
   if(strncasecmp(s, "UID=",      4) == 0) { ret = CNVBCP_UID;       }
   if(strncasecmp(s, "PWD=",      4) == 0) { ret = CNVBCP_PWD;       }

   return(ret);
}



char *
dup_val(char *s)
{
   char *ret = NULL;
   char tmpstr[1024], *t = tmpstr;

   // find the equal sign
   while(*s != '=' && *s != '\0')
      s++;

   // advance past equal
   if(*s == '=')
      s++;

   // copy up to the next semicolon to the temp string
   while(*s != ';' && *s != '\0')
   {
      if(*s == '{')
      {
         // skip open brace
         s++;

         // copy up to closing brace
         while(*s != '}' && *s != '\0')
            *t++ = *s++;

         // skip close brace
         if(*s == '}')
            s++;
      }

      // only copy if this isn't a semicolon trailing the close brace
      if(*s != ';')
         *t++ = *s++;
   }

   // null terminate
   *t = '\0';

   ret = strdup(tmpstr);

   return(ret);
}



char *
cnvbcp_get_odbc_ini(CNVBCP *cnvbcp)
{
   char *ret = NULL;
   FILE *fp;
   char instr[PATH_MAX+128];
   char *tmpstr;

   // Issue the command
   if((fp = popen("odbcinst -j", "r")) == NULL)
   {
      fprintf(cnvbcp->logfp, "Error executing odbcinst:\n");
   }
   else
   {
      // Read command output
      while(fgets(instr, sizeof(instr), fp) != NULL)
      {
         if(strncmp(instr, "SYSTEM DATA SOURCES:", 20) == 0)
         {
            // strip newline
            instr[strlen(instr)-1] = '\0';
            if((tmpstr = strchr(instr, '/')) != NULL)
               ret = strdup(tmpstr);
            break;
         }
      }
      pclose(fp);
   }

   return(ret);
}



char *
cnvbcp_get_database(CNVBCP *cnvbcp)
{
   char *ret = NULL;
   FILE *fp;
   char dsnstr[128];
   char instr[4096];
   char *tmpstr;
   char *odbc_ini;

   if((odbc_ini = cnvbcp_get_odbc_ini(cnvbcp)) != NULL)
   {
      // open the file
      if((fp = fopen(odbc_ini, "r")) == NULL)
      {
         fprintf(cnvbcp->logfp, "Error opening odbc.ini:\n");
      }
      else
      {
         // set up dsn section string to look for
         sprintf(dsnstr, "[%s]", cnvbcp->dsn);

         // look for dsn section
         while(fgets(instr, sizeof(instr), fp) != NULL)
         {
            // consume leading whitespace
            tmpstr = instr;
            while(isspace(*tmpstr) && *tmpstr != '\0')
               tmpstr++;

            if(strncmp(tmpstr, dsnstr, strlen(dsnstr)) == 0)
            {
               // find database line
               while(fgets(instr, sizeof(instr), fp) != NULL)
               {
                  // consume leading whitespace
                  tmpstr = instr;
                  while(isspace(*tmpstr) && *tmpstr != '\0')
                     tmpstr++;

                  // break if a new section is starting
                  if(tmpstr[0] == '[')
                     break;

                  if(strncmp(tmpstr, "Database ",  9) == 0 ||
                     strncmp(tmpstr, "Database\t", 9) == 0 ||
                     strncmp(tmpstr, "Database=",  9) == 0)
                  {
                     // right trim any whitespace
                     tmpstr = &instr[strlen(instr)-1];
                     while(isspace(*tmpstr) && tmpstr >= instr)
                        *tmpstr-- = '\0';

                     if((tmpstr = strchr(instr, '=')) != NULL)
                     {
                        // skip the equal sign
                        tmpstr++;

                        // advance to the first word after the equal sign
                        while(isspace(*tmpstr) && *tmpstr != '\0')
                           tmpstr++;

                        // copy the value
                        if(tmpstr[0] != '\0')
                           ret = strdup(tmpstr);
                     }
                     break;
                  }
               }
            }
         }
         fclose(fp);
      }

      free(odbc_ini);
   }

   return(ret);
}



void
wipearg(char *arg)
{
   char *s = arg;

   while(*s != '\0')
      *s++ = '8';

}



void
cnvbcp_dump_buf(const char *buf, size_t length)
{
   size_t i, j;
#define BYTES_PER_LINE 16

   for (i = 0; i < length; i += BYTES_PER_LINE)
   {
      for (j = 0; j < BYTES_PER_LINE; j++)
      {
         if (j == BYTES_PER_LINE / 2)
            printf("  ");
         else
            printf(" ");
         if (j + i >= length)
            printf("  ");
         else
            printf("%02x", buf[i + j]);
      }

      printf(" |");

      for (j = i; j < length && (j - i) < BYTES_PER_LINE; j++)
      {
         printf("%c", (isprint(buf[j])) ? buf[j] : '.');
      }
      printf("|\n");
   }
   printf("\n");

   fflush(stdout);

}

