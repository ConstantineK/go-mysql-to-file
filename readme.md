# What? 

I have a need for a project that can convert MySQL binlogs into data, this is that project.

## How do I use it?

Building 
``` shell 
go build -o bin/go-mysql-to-file ./cmd/go-mysql-to-file
go build -o bin\go-mysql-to-file.exe .\cmd\go-mysql-to-file
```

Running
```shell 
.\bin\go-mysql-to-file.exe --root .\test\binlogs --out .\output_jsonl
```

Output:
```json
{
  "process_date": "2025-03-23",
  "server_id": 1,
  "log_pos": 450,
  "event_time": "2025-03-21T04:32:59Z",
  "event_type": "insert",
  "schema": "employees",
  "table": "employees",
  "row": [
    500000,
    "1970-01-01",
    "Jane",
    "Doe",
    1,
    "2020-01-01"
  ],
  "binlog_file": "mysql-bin.000007"
}
```
