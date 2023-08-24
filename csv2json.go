package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
)

var (
	separatorStr = flag.String("separator", ",", "Record separator")
	pretty       = flag.Bool("pretty", false, "Pretty format")

	separator rune
)

func main() {
	flag.Parse()

	if len(*separatorStr) != 1 {
		log.Fatalf("--separator must be a single character")
	}

	s := *separatorStr
	separator = rune(s[0])

	args := flag.Args()
	if len(args) < 1 {
		log.Fatalf("usage: %s <input.csv> [input2.csv...]", os.Args[0])
	}

	for _, filename := range args {
		processCSV(filename)
	}
}

func processCSV(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("open file err: %s", err)
	}
	defer f.Close()

	dr, err := decompressReader(f)
	if err != nil {
		log.Fatalf("decompressReader err: %s", err)
	}

	r := csv.NewReader(dr)
	r.Comma = separator
	var header []string
	header, err = r.Read()
	if err != nil {
		log.Fatalf("csv read header err: %s", err)
	}

	missingHeaders := make(map[string]struct{})
	regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	for i, h := range header {
		var uniqPart int
		h = strings.ToLower(h)
		h = strings.TrimSpace(h)
		h = regex.ReplaceAllString(h, "_")

		uniqH := h
		for _, existing := missingHeaders[uniqH]; existing; _, existing = missingHeaders[uniqH] {
			uniqPart++
			uniqH = fmt.Sprintf("%s_%d", h, uniqPart)
			if uniqPart > 100 {
				log.Fatalf("too many name collisions for %s, something is probably wrong with your csv file", h)
			}
		}
		header[i] = uniqH
		missingHeaders[uniqH] = struct{}{}
	}

	rec := make(map[string]string)
	enc := json.NewEncoder(os.Stdout)

	if *pretty {
		enc.SetIndent("", "  ")
	}

	for {
		line, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Read err %s: %s", filename, err)
		}

		for i, v := range line {
			if v == "" {
				continue
			}
			rec[header[i]] = v
		}

		enc.Encode(rec)

		for k := range rec {
			delete(rec, k)
		}
	}
}

func decompressReader(f *os.File) (io.Reader, error) {
	if strings.HasSuffix(f.Name(), ".gz") {
		return gzip.NewReader(f)
	}

	return f, nil
}
