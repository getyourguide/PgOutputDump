# PgOutputDump

A simple CLI to read pgoutput format from a replication slot and optionally dump
it to a file. This file can later be read back using the same CLI.

# Usage

## Read replication stream from live database

Note: Java >= 11 is required.

```bash
java -jar target/PgOutputDump-jar-with-dependencies.jar \
  --host=localhost \
  --database=my_databsase \
  --user=master \
  --password=my_password \
  --slot=my_slot \
  --publication=a_replication \
  -c 10
```

To dump a copy of the binary stream, use the `--file output.bin` option. The file is simply written
as the payload size (int32) followed by the payload itself for each received replication event.

```bash

## Read replication stream from a local file

```bash
java -jar target/PgOutputDump-jar-with-dependencies.jar \
  --file output.bin
```

Note: This tool will not create nor the slot nor the replication, they need to exist
beforehand.
