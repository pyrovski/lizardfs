/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <stdlib.h>
#include <cstdint>
#include <cstring>
#include <vector>
#include <sys/types.h>
#include <sys/uio.h>

// Holds pointers to owned an unowned memory blocks. Only frees owned blocks
// on destruction. Data in an OutputBuffer is read-only append-only until it is
// destroyed.
class OutputBuffer {
public:
	enum WriteStatus {
		WRITE_DONE,
		WRITE_AGAIN,
		WRITE_ERROR
	};

	OutputBuffer() = default;
	~OutputBuffer();

	// Maps data into the buffer without copying.
	// Mapped data must outlive the buffer.
	// Returns the number of bytes mapped.
	ssize_t mapIntoBuffer(const void *mem, size_t len);

	// Maps data into the buffer without copying.
	// Mapped data must outlive the buffer.
	// Returns the number of bytes mapped.
	ssize_t mapIntoBuffer(const std::vector<uint8_t> &data);

	// Copies (appends) 'len' bytes at offset 'offset' from 'inputFileDescriptor'.
	// Returns the number of bytes copied.
	// Prefer mapping extant data over copying.
	// TODO(peb): This function was broken for short reads with non-nullptr 'offset'; add a test.
	ssize_t copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset);

	// Returns the number of bytes copied.
	ssize_t copyIntoBuffer(const void *mem, size_t len);

	// Returns the number of bytes copied; i.e., the length of 'mem'.
	ssize_t copyIntoBuffer(const std::vector<uint8_t>& mem) {
		return copyIntoBuffer(mem.data(), mem.size());
	}

	// Writes buffer data to file descriptor. If 'retry' is true, will issue
	// multiple writes as necessary. If 'retry' is false, returns WRITE_AGAIN on
	// short writes.
	WriteStatus writeOutToAFileDescriptor(int outputFileDescriptor, bool retry=true);

	// Returns the number of unflushed bytes in the buffer.
	size_t bytesInABuffer() const;

private:
	std::vector<struct iovec> buffers_;
	// owned_[i] is true if this object owns buffers_[i]. Should always be the same length as 'buffers_'.
	std::vector<bool> owned_; 
	// Offset of the first unflushed byte in the buffer's virtual array.
	size_t bufferUnflushedDataFirstOffset_ = 0;
	// Total size in bytes of all owned an unowned buffers.
	size_t size_ = 0;
};
