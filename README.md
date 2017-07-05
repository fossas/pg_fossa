# pg_fossa

PostgreSQL extension for Fossa

## Install

1. Add pg_fossa to your extension share directory: `cp ./* $(pg_config --sharedir)/extension/`
2. Execute `CREATE EXTENSION pg_fossa` in your PostgreSQL database

## Upgrade

1. Add pg_fossa to your extension share directory: `cp ./* $(pg_config --sharedir)/extension/`
2. Execute `ALTER EXTENSION pg_fossa UPDATE` in your PostgreSQL database

## Check Version

To see the version of this plugin installed, execute `SELECT fossa_version()` in PostgreSQL.

Another option is to try interrogating the description via `\dfnS+ fossa_dependencies`.
