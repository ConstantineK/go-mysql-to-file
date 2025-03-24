package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"go-mysql-to-file/internal/parser"
)

func main() {
	root := flag.String("root", "./binlogs", "Root directory containing binlog folders or files")
	out := flag.String("out", "./output", "Output directory for parsed JSONL files")
	flag.Parse()

	if err := os.MkdirAll(*out, 0755); err != nil {
		fmt.Printf("Failed to create output dir: %v\n", err)
		return
	}

	err := filepath.Walk(*root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if matched, _ := filepath.Match("mysql-bin.*", filepath.Base(path)); matched {
			fmt.Println("Parsing:", path)
			return parser.ParseBinlogFile(path, *out)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking through files: %v\n", err)
	}
}
