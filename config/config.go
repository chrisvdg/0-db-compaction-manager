package config

import (
	"fmt"
	"io/ioutil"

	"github.com/ghodss/yaml"
)

// ZDBCompactionManagerFromYAML returns a ZDBCompactionManager from provided YAML file
func ZDBCompactionManagerFromYAML(path string) (*ZDBCompactionManager, error) {
	cfg := new(ZDBCompactionManager)
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Error while reading config file: %v", err)
	}
	err = yaml.Unmarshal(raw, cfg)

	if err != nil {
		return nil, err
	}

	err = cfg.Validate()
	if err != nil {
		return nil, err
	}

	cfg.setDefaults()

	return cfg, nil
}

// ZDBCompactionManager represents a ZDB Compaction Manager configuration
type ZDBCompactionManager struct {
	Schedule   string `json:"schedule"`
	BackendDir string `json:"backend_dir"`
	ListenAddr string `json:"listen_address"`
	ListenPort string `json:"listen_port"`
	Mode       string `json:"mode"`
	DataSize   string `json:"data_size"`
	Verbose    bool   `json:"verbose"`
}

// Validate validates a ZDBCompactionManager config
func (z *ZDBCompactionManager) Validate() error {
	if z.Schedule == "" {
		return fmt.Errorf("empty schedule")
	}

	return nil
}

func (z *ZDBCompactionManager) setDefaults() {
	if z.BackendDir == "" {
		z.BackendDir = "./zdb"
	}

	if z.ListenAddr == "" {
		z.ListenAddr = "0.0.0.0"
	}

	if z.ListenPort == "" {
		z.ListenPort = "9900"
	}
}
