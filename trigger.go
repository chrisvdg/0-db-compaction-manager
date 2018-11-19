package main

import "syscall"

func NewCompactingTrigger(zdb *ZDB) *CompactingTrigger {
	var ct CompactingTrigger
	ct.zdb = zdb
	return &ct
}

// CompactingTrigger triggers compacting of the 0-db
type CompactingTrigger struct {
	zdb *ZDB
}

// Run implements cron.Job.Run
func (ct *CompactingTrigger) Run() {
	ct.zdb.compactTriggered = true
	ct.zdb.cmd.Process.Signal(syscall.SIGINT)
}
