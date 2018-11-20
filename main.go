package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/robfig/cron"
)

func init() {
	// Enable linenumbers in log messages
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var (
	zdbBin           = "zdb"
	zdbCompactionBin = "zdb-compaction"
	zdbReindexerBin  = "index-rebuild"
)

func main() {
	zdbBackend := flag.String("dir", "zdb", "0-db backend folder (will contain index and data folders)")
	schedule := flag.String("schedule", "0 0 4 * * *", "Schedule when compaction will take place (default: every day at 4am)")
	port := flag.String("port", "9900", "Listen port")
	listen := flag.String("listen", "0.0.0.0", "Listen address")
	mode := flag.String("mode", "", "0-db mode")
	datasize := flag.String("datasize", "", "Maximum datafile size before split (default: 256.00 MB)")
	verbose := flag.Bool("verbose", false, "Verbose output")

	flag.Parse()

	if *verbose {
		fmt.Printf("schedule: %s\n", *schedule)
	}

	zdb, err := NewZDB(*zdbBackend, *listen, *port, *datasize, *mode, *verbose)
	if err != nil {
		log.Fatal(err)
	}

	// Run compactor trigger in background
	err = scheduleCompacting(*schedule, zdb)
	if err != nil {
		log.Fatal(err)
	}

	// Run zdb backend
	err = zdb.Run()

	if err != nil {
		log.Fatal(err)
	}
}

//scheduleCompacting schedules compacting
func scheduleCompacting(schedule string, zdb *ZDB) error {
	c := cron.New()

	job := NewCompactingTrigger(zdb)
	err := c.AddJob(schedule, job)
	if err != nil {
		return err
	}
	c.Start()

	return nil
}
