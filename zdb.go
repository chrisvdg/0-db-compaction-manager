package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

// NewZDB returns a new ZDB
func NewZDB(backendDir, listenAddress, listenport, datasize, mode string, verbose bool) (*ZDB, error) {
	// check if backend dir exists, if not create it
	if _, err := os.Stat(backendDir); os.IsNotExist(err) {
		os.MkdirAll(backendDir, os.ModePerm)
	}

	// check latest data en index dir
	// data and index dirs are assumed to be index-0, data-0
	files, err := ioutil.ReadDir(backendDir)
	if err != nil {
		return nil, err
	}

	highestIndex := int64(-1)
	for _, f := range files {
		if f.IsDir() {
			if strings.Contains(f.Name(), "index") {
				subs := strings.Split(f.Name(), "-")
				// check 2 parts
				if len(subs) != 2 {
					continue
				}
				// check first part is index
				if subs[0] != "index" {
					continue
				}
				// check second part is numeric
				var index int64
				if index, err = strconv.ParseInt(subs[1], 10, 64); err != nil {
					continue
				}
				// check if equivalent data dir exists
				dataDir := fmt.Sprintf("data-%d", index)
				if !dirInFiles(files, dataDir) {
					continue
				}

				if index > highestIndex {
					highestIndex = index
				}
			}
		}
	}
	dataDir := path.Join(backendDir, fmt.Sprintf("data-%d", highestIndex))
	indexDir := path.Join(backendDir, fmt.Sprintf("index-%d", highestIndex))
	if highestIndex == -1 {
		highestIndex = 0
		dataDir = path.Join(backendDir, fmt.Sprintf("data-%d", highestIndex))
		indexDir = path.Join(backendDir, fmt.Sprintf("index-%d", highestIndex))
		err := os.MkdirAll(dataDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
		err = os.MkdirAll(indexDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	var zdb ZDB
	zdb.backendDir = backendDir
	zdb.backendIndex = highestIndex
	zdb.dataDir = dataDir
	zdb.indexDir = indexDir
	zdb.listenAddress = listenAddress
	zdb.listenPort = listenport
	zdb.mode = mode
	zdb.verbose = verbose
	zdb.datasize = datasize
	zdb.compactTriggered = false
	zdb.running = false

	return &zdb, nil
}

func dirInFiles(files []os.FileInfo, dirName string) bool {
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		if f.Name() == dirName {
			return true
		}
	}

	return false
}

// ZDB represents the 0-db instance
type ZDB struct {
	backendDir       string
	dataDir          string
	indexDir         string
	listenAddress    string
	listenPort       string
	mode             string
	datasize         string
	verbose          bool
	compactTriggered bool
	backendIndex     int64
	cmd              *exec.Cmd
	running          bool
}

// Run runs a zdb instance
// If stopped by compactions it will restart
// If stopped by anything else, the method will exit
func (zdb *ZDB) Run() error {
	for {
		args := []string{
			"--data", zdb.dataDir,
			"--index", zdb.indexDir,
			"--listen", zdb.listenAddress,
			"--port", zdb.listenPort,
		}

		if zdb.mode != "" {
			args = append(args, "--mode", zdb.mode)
		}
		if zdb.datasize != "" {
			args = append(args, "--datasize", zdb.mode)
		}
		if zdb.verbose {
			args = append(args, "--verbose")
		}

		zdb.cmd = exec.Command(zdbBin, args...)
		zdb.cmd.Stdout = os.Stdout
		zdb.cmd.Stderr = os.Stderr

		fmt.Printf("%s: Running zdb\n", time.Now().Format(timestampFormat))
		zdb.running = true
		err := zdb.cmd.Run()
		zdb.running = false

		if zdb.compactTriggered {
			fmt.Printf("%s: Compacting zdb\n", time.Now().Format(timestampFormat))
			err := zdb.Compact()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: Compacting failed: %s\n", time.Now().Format(timestampFormat), err)
			}
			zdb.compactTriggered = false
			continue
		}

		return err
	}
}

// Compact compacts the zdb data
func (zdb *ZDB) Compact() error {
	if zdb.running {
		return fmt.Errorf("zdb is still running")
	}

	newIndex := zdb.backendIndex + 1
	newDataDir := path.Join(zdb.backendDir, fmt.Sprintf("data-%d", newIndex))
	newIndexDir := path.Join(zdb.backendDir, fmt.Sprintf("index-%d", newIndex))
	err := os.MkdirAll(newDataDir, os.ModePerm)
	if err != nil {
		os.RemoveAll(newDataDir)
		return err
	}
	err = os.MkdirAll(newIndexDir, os.ModePerm)
	if err != nil {
		os.RemoveAll(newDataDir)
		os.RemoveAll(newIndexDir)
		return err
	}

	files, err := ioutil.ReadDir(zdb.indexDir)
	if err != nil {
		os.RemoveAll(newDataDir)
		os.RemoveAll(newIndexDir)
		return err
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		if !isNamespace(path.Join(zdb.indexDir, f.Name())) {
			continue
		}
		namespace := f.Name()

		// run data compaction
		compactionArgs := []string{
			"--data", zdb.dataDir,
			"--target", newDataDir,
			"--namespace", namespace,
		}
		compactionCmd := exec.Command(zdbCompactionBin, compactionArgs...)
		compactionCmd.Stdout = os.Stdout
		compactionCmd.Stderr = os.Stderr
		err = compactionCmd.Run()
		if err != nil {
			os.RemoveAll(newDataDir)
			os.RemoveAll(newIndexDir)
			return err
		}

		// run reindexer
		reindexerArgs := []string{
			"--data", newDataDir,
			"--index", newIndexDir,
			"--namespace", namespace,
			"--template", path.Join(zdb.indexDir, namespace, "zdb-namespace"),
		}
		reindexerCmd := exec.Command(zdbReindexerBin, reindexerArgs...)
		reindexerCmd.Stdout = os.Stdout
		reindexerCmd.Stderr = os.Stderr
		err = reindexerCmd.Run()
		if err != nil {
			os.RemoveAll(newDataDir)
			os.RemoveAll(newIndexDir)
			return err
		}
	}
	oldDataDir := zdb.dataDir
	oldIndexDir := zdb.indexDir

	zdb.backendIndex = newIndex
	zdb.indexDir = newIndexDir
	zdb.dataDir = newDataDir

	// cleanup old dirs
	err = os.RemoveAll(oldDataDir)
	if err != nil {
		return err
	}
	return os.RemoveAll(oldIndexDir)
}

// isNamespace checks if path contains file zdb-namespace
func isNamespace(path string) bool {
	files, _ := ioutil.ReadDir(path)

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if f.Name() == "zdb-namespace" {
			return true
		}
	}

	return false
}
