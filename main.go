package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/chrisvdg/0-db-compaction-manager/config"

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
	timestampFormat  = "15:04:05 02/01/06"
)

func main() {
	cfgFile := flag.String("config", "config.yaml", "Config file location")

	flag.Parse()

	cfg, err := config.ZDBCompactionManagerFromYAML(*cfgFile)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Schedule: %s\n", cfg.Schedule)

	zdb, err := NewZDB(cfg.BackendDir, cfg.ListenAddr, cfg.ListenPort, cfg.DataSize, cfg.Mode, cfg.Verbose)
	if err != nil {
		log.Fatal(err)
	}

	// Run compactor trigger in background
	err = scheduleCompacting(cfg.Schedule, zdb)
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
