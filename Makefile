###################################################################

###################################################################

###################################################################
# DO NOT REMOVE THIS include DIRECTIVE
include /usr/local/conv/etc/make/conv_defaults.mak
###################################################################

###################################################################
# you must define at least one of these two macros.  
#     C_SRCS is for .c files
#    SC_SRCS is for .sc files
C_SRCS=cnvbcp.c
SC_SRCS=


###################################################################
# add any additional pre-compiled .o files you need to link 
ADD_OBJS=

###################################################################
# add any precompiler option overrides to this macro
PCADD=

###################################################################
# add any additional compiler flags to this macro 
MFLAGS=

###################################################################
# add any additional library paths (-Ldir) to this macro 
LIBADD=

###################################################################
# add any additional libraries to link (-llibname) to this macro
EXTRA_LIBS=-lDOParser -lprogressbar -ldate -lsybdb
DEBUG_EXTRA_LIBS=-lDOParser_dbg -lprogressbar_dbg -ldate_dbg -lsybdb

###################################################################
# add any additional include directories (-Idir) to this macro
INC_DIR=

###################################################################
# define this macro to override the default install directory
# for the items defined in TARGET
#     i.e.  for libraries, INSTALLDIR=../lib
#      or   for binaries, INSTALLDIR=../bin
# the defaults will produce output in the current directory
INSTALLDIR=../../bin

###################################################################
# define this macro to override the default intermediate (.o) 
# directory for the items defined in SC_SRCS and C_SRCS
#     i.e.  OBJDIR=../obj
# the defaults will produce output in the current directory
OBJDIR=.

###################################################################
# the following 2 macros MUST be defined
# define the final desired target names
TARGET=cnvbcp

LOCAL_INSTALL=install_scripts
LOCAL_UNINSTALL=uninstall_scripts

###################################################################
# DO NOT REMOVE THIS include DIRECTIVE
include /usr/local/conv/etc/make/conv_rules.mak
###################################################################

SCRIPTS = dataload.sh

install_scripts:
	@for script in $(SCRIPTS); do echo "$(CP) -f $$script $(INSTALLDIR)" ; $(CP) -f $$script $(INSTALLDIR) ; done

uninstall_scripts:
	@for script in $(SCRIPTS); do echo "$(RMF) $(INSTALLDIR)/$$script "; $(RMF) $(INSTALLDIR)/$$script ; done

