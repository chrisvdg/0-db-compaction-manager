# 0-db-compaction-manager

Compaction manager for 0-db (https://github.com/threefoldtech/0-db)

## Usage

```sh
# All parameters are optional
0-db-compaction-manager \
    -schedule "0 0 * * * *" \ # schedule compacting hourly at start of the hour, default "0 0 4 * * *" (every day at 4am at systems timezone).
    -datasize 123 \ # datasize in bytes, default is zdb's default (256 MB)
    -dir /tmp/zdb \ # backend directory, default it will create a zdb folder in current dir
    -mode \ # zdb operating mode, defaults to zdb's default mode
    -listen "127.0.0.1" \ # zdb's listening address, default "0.0.0.0"
    -port 9900 \ # zdb's listening port, default 9900
    -verbose # enable verbose output
```

More information about the schedule formatting: https://godoc.org/github.com/robfig/cron#hdr-CRON_Expression_Format

## Backend dir

Inside the specified (or default) backend dir, the compaction manager will create 2 folders in the following format:
```
    - data-<index>  
    - index-<index>  
```

The index is an integer that indicates the  version of the backend, it is increased every time the compaction has run.  
If compaction has run successfully the previous version is removed.  
If not, the newly created version in preparation will be delete and the previous version will be used again.

When starting the compaction manager, it will pick out the index and data dirs with the highest common index to use as zdb backend.
