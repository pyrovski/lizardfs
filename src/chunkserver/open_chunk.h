/*
  Copyright 2016 Skytechnology sp. z o.o..

  This file is part of LizardFS.

  LizardFS is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, version 3.

  LizardFS is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
*/

#pragma once

#include "common/platform.h"

#include <fcntl.h>
#include <unistd.h>
#include <array>

#include "chunkserver/chunk.h"
#include "chunkserver/hddspacemgr.h"

#include <unistd.h>

/*!
 * Class representing an open chunk with acquired resources.
 * In its destructor, it is assumed that hdd_chunk_find/hdd_chunk_trylock was called
 * and chunk is in locked state, so its resources can be safely freed.
 */
class OpenChunk {
 public:
  OpenChunk() = default;
  
 OpenChunk(Chunk *chunk) : chunk_(chunk), fd_(chunk ? chunk->fd : -1) {
	sassert(chunk);
	sassert(fd_ >= 0);
	if (chunk && chunk->chunkFormat() == ChunkFormat::MOOSEFS) {
	  crc_.reset(new MooseFSChunk::CrcDataContainer{{}});
	}
	/* struct stat statbuf; */
	/* memset(&statbuf, 0, sizeof(statbuf)); */
	/* zassert(fstat(fd, &statbuf)); */
	int mapFlags = PROT_READ;
	const int openFlags = fcntl(fd_, F_GETFD);
	eassert(openFlags >= 0);
	if (openFlags & O_RDWR) {
	  mapFlags |= PROT_WRITE;
	}
	/* map_ = mmap(nullptr, statbuf.st_size, mapFlags, MAP_SHARED, fd_, 0); */
	// TODO(peb): map the maximum size of the chunk so we don't have to remap later.
	map_ = mmap(nullptr, , mapFlags, MAP_SHARED, fd_, 0);
	sassert(map_ != MAP_FAILED);
  }

  OpenChunk(OpenChunk &&other) noexcept
	: chunk_(other.chunk_), fd_(other.fd_), crc_(std::move(other.crc_), map_(other.map_), mapSize_(other.mapSize_) {
	other.chunk_ = nullptr;
	other.fd_ = -1;
	other.map_ = nullptr;
	other.mapSize_ = 0;
  }
  
  OpenChunk(const OpenChunk &other) = delete;
  operator=(const OpenChunk &rhs) = delete;
  operator=(OpenChunk &&rhs) = delete;

  /*!
   * OpenChunk destructor.
   * It is assumed that chunk_, if it exists, is properly locked.
   */
  ~OpenChunk() {
	// Order between munmap() and close() doesn't matter.
	// chunk_ may not be set due to prior calls to purge().
	if (chunk_) {
	  if (chunk_->fd >= 0) {
		if (::close(chunk_->fd) < 0) {
		  hdd_error_occured(chunk_);
		  lzfs_silent_errlog(LOG_WARNING,"open_chunk: file:%s - close error",
							 chunk_->filename().c_str());
		  hdd_report_damaged_chunk(chunk_->chunkid, chunk_->type());
		}
		if (munmap(map_, mapSize_)) {
		  lzfs_silent_errlog(LOG_WARNING,"open_chunk: file:%s - munmap error",
							 chunk_->filename().c_str());
		  hdd_report_damaged_chunk(chunk_->chunkid, chunk_->type());
		}
	  }
	  chunk_->fd = -1;
	  hdd_chunk_release(chunk_);
	} else if (fd_ >= 0) {
	  ::close(fd_);
	  munmap(map_, mapSize_);
	}
  }

  /*!
   * Try to lock chunk in order to be able to remove it.
   * \return true if chunk was successfully locked and can be removed, false otherwise.
   */
  bool canRemove() {
	return hdd_chunk_trylock(chunk_);
  }

  /*!
   * Remove a connection to chunk and prepare its resources to be freed.
   * This function is called if chunk is to be inaccessible (deleted) in near future,
   * but its resources (e.g. file descriptor) still need to be freed properly.
   */
  void purge() {
	assert(chunk_);
	fd_ = chunk_->fd;
	chunk_ = nullptr;
  }

  uint8_t *crc_data() const {
	assert(crc_);
	return crc_->data();
  }

  void *map(size_t *mapSize = nullptr) const {
	if (mapSize != nullptr) {
	  *mapSize = mapSize_;
	}
	return map_;
  }

 private:
  Chunk *chunk_ = nullptr;
  int fd_ = -1;
  // TODO(peb): track mapped size, which will change as the chunk grows or is
  // truncated. Will need to call mremap().
  // TODO(peb): handle signals that arise from mmap usage.
  void * map_ = nullptr;
  size_t mapSize_ = 0;
  std::unique_ptr<MooseFSChunk::CrcDataContainer> crc_;
};
