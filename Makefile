MODULE_big = pg_table_bloat
OBJS = \
        $(WIN32RES) \
        pg_bloat.o

EXTENSION = pg_table_bloat
DATA = pg_table_bloat--1.0.sql
PGFILEDESC = "pg_table_bloat - Table dead tuples"
REGRESS = pg_table_bloat

ifndef OLD_INSTALL  # Random variable name???
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_table_bloat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif