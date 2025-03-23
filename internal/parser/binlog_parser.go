package parser

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"os"
	"path/filepath"
	"time"
)

type OutputRow struct {
	ProcessDate string      `json:"process_date"`
	ServerID    uint32      `json:"server_id"`
	LogPos      uint32      `json:"log_pos"`
	EventTime   string      `json:"event_time"`
	EventType   string      `json:"event_type"`
	Schema      string      `json:"schema"`
	Table       string      `json:"table"`
	RowData     interface{} `json:"row"`
	BinlogFile  string      `json:"binlog_file"`
}

func inspectMagicHeader(file *os.File) error {
	const magicLen = 4
	buf := make([]byte, 256)

	n, err := file.Read(buf)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}
	if n < magicLen {
		return fmt.Errorf("file too short: got %d bytes", n)
	}
	if buf[0] != 0xfe || buf[1] != 0x62 || buf[2] != 0x69 || buf[3] != 0x6e {
		fmt.Printf("First %d bytes:\n%s\n", n, hex.Dump(buf[:n]))
		return fmt.Errorf("invalid magic header: got %x, expected fe62696e", buf[:4])
	}
	_, _ = file.Seek(0, 0)
	return nil
}

func ParseBinlogFile(path string, outDir string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := inspectMagicHeader(f); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()

	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(false)

	tables := map[uint64]*replication.TableMapEvent{}
	base := filepath.Base(path)
	outputPath := filepath.Join(outDir, base+".jsonl")

	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	processDate := time.Now().Format("2006-01-02")

	return parser.ParseFile(path, 0, func(ev *replication.BinlogEvent) error {
		h := ev.Header
		if h == nil {
			return nil
		}

		switch e := ev.Event.(type) {
		case *replication.TableMapEvent:
			tables[e.TableID] = e

		case *replication.RowsEvent:
			table := tables[e.TableID]
			if table == nil {
				return nil
			}

			var typ string
			switch h.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				typ = "insert"
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				typ = "update"
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				typ = "delete"
			default:
				return nil
			}

			eventTime := time.Unix(int64(h.Timestamp), 0).UTC().Format(time.RFC3339)

			for _, row := range e.Rows {
				outRow := OutputRow{
					ProcessDate: processDate,
					ServerID:    h.ServerID,
					LogPos:      h.LogPos,
					EventTime:   eventTime,
					EventType:   typ,
					Schema:      string(table.Schema),
					Table:       string(table.Table),
					RowData:     row,
					BinlogFile:  base,
				}
				if err := enc.Encode(outRow); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
