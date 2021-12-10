# PgOutputDump
A simple CLI to read pgoutput format from a replication slot

# Usage

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

This tool will not create nor the slot nor the replication, they need to exist 
beforehand.
