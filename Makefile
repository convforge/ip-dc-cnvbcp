###################################################################
# DO NOT REMOVE THIS include DIRECTIVE
include /usr/local/conv/etc/make/conv_defaults.mak
###################################################################

###################################################################
# you must define at least one of these two macros.  
# C_SRCS=    (for .c files)
# SC_SRCS=   (for .sc files)
C_SRCS=cnvbcp.c

###################################################################
# add any additional files you want to check as dependecies that
# should cause the target to be rebuilt
# EXTRA_DEPS=

###################################################################
# add any additional pre-compiled .o files you need to link 
# EXTRA_OBJS=

###################################################################
# define this marco if your program uses Pro*C (oracle calls),
# to make sure the Oracle libraries get linked in
# eg.
#    ORACLE_LDFLAGS=-L$(ORACLE_HOME)/lib $(ORACLE_LIBS)
# ORACLE_LDFLAGS=

###################################################################
# define this marco if your program uses Pro*C (oracle calls),
# to add any Pro*C precompiler option overrides to the compile
# PCFLAGS=

###################################################################
# add any additional compiler flags to this macro 
# MFLAGS=

###################################################################
# add any additional library paths (-Ldir) to this macro 
# EXTRA_LIBDIRS=

###################################################################
# add any additional libraries to link (-llibname) to this macro
# EXTRA_LIBS=
# EXTRA_DEBUG_LIBS=
EXTRA_LIBS=-lDOParser -lprogressbar -lcnvdate -lsybdb
EXTRA_DEBUG_LIBS=-lDOParser_dbg -lprogressbar_dbg -lcnvdate_dbg -lsybdb

###################################################################
# add any additional include directories (-Idir) to this macro
# EXTRA_INCDIRS=

###################################################################
# define this macro to override the default intermediate
# directory (./) for the C_SRCS and SC_SRCS
# eg.
#     OBJDIR=../obj
# OBJDIR=

###################################################################
# define this macro to override the default install directory
# (../bin) for the TARGET
# eg.
#    INSTALLDIR=../bin    (for binaries)
#    INSTALLDIR=../lib    (for libraries)
# INSTALLDIR=
INSTALLDIR=../../bin

###################################################################
# this macro MUST be defined
# define the final desired target names
# TARGET=
TARGET=cnvbcp

###################################################################
# define this macro for libraries supplying header files
# LIBRARY_HEADER=

###################################################################
# define this macro for targets supplying additional files such as
# scripts or templates
# eg.
#    EXTRA_INSTALLS=install_scripts
# EXTRA_INSTALLS=
EXTRA_INSTALLS=install_scripts

###################################################################
# define this macro for targets supplying additional files such as
# scripts or templates
# eg.
#    EXTRA_UNINSTALLS=uninstall_scripts
# EXTRA_UNINSTALLS=
EXTRA_UNINSTALLS=uninstall_scripts

###################################################################
# DO NOT REMOVE THIS include DIRECTIVE
include /usr/local/conv/etc/make/conv_rules.mak
###################################################################

###################################################################
# define macros and recipes required by the EXTRA_INSTALLS and
# EXTRA_UNINSTALLS definitions above
# eg.
#    SCRIPTS = the_script.sh
#
#    install_scripts:
#    	@for script in $(SCRIPTS); do echo "$(CP) -f $$script $(INSTALLDIR)" ; $(CP) -f $$script $(INSTALLDIR) ; done
#
#    uninstall_scripts:
#    	@for script in $(SCRIPTS); do echo "$(RMF) $(INSTALLDIR)/$$script "; $(RMF) $(INSTALLDIR)/$$script ; done
SCRIPTS = dataload.sh

install_scripts:
	@for script in $(SCRIPTS); do echo "$(CP) -f $$script $(INSTALLDIR)" ; $(CP) -f $$script $(INSTALLDIR) ; done

uninstall_scripts:
	@for script in $(SCRIPTS); do echo "$(RMF) $(INSTALLDIR)/$$script "; $(RMF) $(INSTALLDIR)/$$script ; done

