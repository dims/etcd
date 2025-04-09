// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	bolt "go.etcd.io/bbolt"
)

// syscall.MAP_POPULATE on linux 2.6.23+ does sequential read-ahead
// which can speed up entire-database read with boltdb. We want to
// enable MAP_POPULATE for faster key-value store recovery in storage
// package. If your kernel version is lower than 2.6.23
// (https://github.com/torvalds/linux/releases/tag/v2.6.23), mmap might
// silently ignore this flag. Please update your kernel to prevent this.
var boltOpenOptions = &bolt.Options{
	MmapFlags:      syscall.MAP_POPULATE,
	NoFreelistSync: true,
	OpenFile: func(path string, flag int, perm os.FileMode) (*os.File, error) {
		if os.Getenv("ETCD_TMPFS") != "" {
			return os.OpenFile(path, flag, perm)
		}
		tempDir, err := os.MkdirTemp("/mnt", "tmpfs-")
		if err != nil {
			fmt.Println("Error creating tmpfs directory:", err)
			return nil, err
		}
		fmt.Println("Temporary directory created:", tempDir)
		err = unix.Mount("tmpfs", tempDir, "tmpfs", 0, "")
		if err != nil {
			fmt.Println("Error mounting tmpfs:", err)
			return nil, err
		}
		fmt.Println("Mounted tmpfs at", tempDir)

		fileName := strings.ReplaceAll(filepath.ToSlash(path), "/", "_")
		pathNew := filepath.Join(tempDir, fileName)
		return os.OpenFile(pathNew, flag, perm)
	},
}

func (bcfg *BackendConfig) mmapSize() int { return int(bcfg.MmapSize) }
