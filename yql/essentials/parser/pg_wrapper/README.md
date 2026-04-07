
Directory postgresql/ contains PostgreSQL sources that were automatically patched to make all global variables to become thread-local ones.
This makes it possible to use pg functions in multi-thread environment.

Also, there are some additional commits to postgresql/ (mostly backports from newer PostgreSQL versions
or renames of PostgreSQL C functions when corresponding function is reimplemented outside of postgresql/ directory/)

Base version of PostgreSQL can be seen in copy_src.sh script (VERSION variable).

copy_src.sh script downloads, configures and builds static postgresql.a library from original PostgreSQL sources with minor patches (source.patch).
After that, it finds all global r/w symbols by analyzing output of "objdump postgresql.a" command.
Then it copies all necessary *.c and *.h files to postgresql/ directory, automatically changing all static variables to thread-local ones.

How to upgrade to newer PostgreSQL:

1. Make sure you have folloing packages installed (assuming you have Ubuntu Linux):

libicu-dev
icu-devtools
pkg-config
liblz4-dev
libreadline-dev
libssl-dev
libxml2-dev
libossp-uuid-dev
build-essential
zlib1g-dev

2. On clean repository perform following command:

  ./copy_src.sh && arc diff -R postgresql  > local_changes.patch

  In file local_changes.patch you will get all changes applied to postgresql/ directory which are not part of automated patching by copy_src.sh

3. Cleanup repositry:

   arc checkout .

4. Bump PostgreSQL version in copy_src.sh (it is recommended not to do big jumps here)

5. Run ./copy_src.sh

6. Assuming compilation and automatic patching were successful, apply local changes collected on step 2:

   patch -p4 < local_changes.patch

   Resolve possible conflicts. Usually, conflicts arise due to some already backported changes

   check for .rej/.orig files:

   find . -name "*.rej"
   find . -name "*.orig"

   remove .rej/.orig files:

   find . -name "*.rej" | xargs rm
   find . -name "*.orig" | xargs rm

7. Update pg_catalog data

   (cd ../../tools/pg_catalog_dump/ && ya make --build=relwithdebinfo && YQL_ALLOW_ALL_PG_FUNCTIONS=1 ./pg_catalog_dump | jq > dump.json)

8. Regenerate Arrow postgresql kernels

   ./generate_kernels.py

9. Make sure that resulting pg_wrapper library compiles and passes minimal tests

   ya make --build=relwithdebinfo -tA -C ut -C test -C ../../sql/pg/ut

10. Verify that all global variables in PostgreSQL-originated sources are accounted for

   ./verify.sh

   Should output OK

11. Remove local_changes.patch and build directory

    rm -rf build
    rm local_changes.patch

12. Submit PR (do not forget to arc add all new files created in postgresql/ directory - they should also present in pg_sources.inc)
