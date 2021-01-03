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

#include "common/platform.h"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <ios>
#include <stdexcept>
#include <sys/uio.h>

#include "chunkserver/output_buffer.h"
#include "common/crc.h"
#include "common/massert.h"
#include "devtools/request_log.h"

// TODO(peb): investigate TCP cork
OutputBuffer::WriteStatus OutputBuffer::writeOutToAFileDescriptor(int outputFileDescriptor) {
  sassert(owned_.size() == buffers_.size());
  sassert(bufferUnflushedDataFirstOffset_ <= size_);
  if (bufferUnflushedDataFirstOffset_ == size_) {
	return WRITE_DONE;
  }
  // We have at least one byte to write.
  // Find first unflushed iovec
  size_t unflushedIndex = 0;
  // Virtual offset of buffers_[unflushedIndex]
  size_t unflushedOffset = 0;
  while (unflushedOffset < bufferUnflushedDataFirstOffset_ && unflushedIndex < buffers_.size()) {
	if (bufferUnflushedDataFirstOffset_ < unflushedOffset + buffers_[unflushedIndex].iov_len) {
	  // bufferUnflushedDataFirstOffset_ is in this iovec.
	  break;
	}
	unflushedOffset += buffers_[unflushedIndex].iov_len;
	++unflushedIndex;
  }
  // 'unflushedOffset' is the virtual offset corresponding to the start of buffers_[offset_index].
  if (unflushedIndex >= buffers_.size()) {
	// Did not find the requisite iovec.
	return WRITE_ERROR;
  }
  while (bufferUnflushedDataFirstOffset_ < size_) {
	if (unflushedOffset < bufferUnflushedDataFirstOffset_) {
	  // We need to issue a partial iovec write or modify the iovec.
	  // If we own the data, we can't modify the iovec because we have to free the
	  // memory later. If we don't own the data, just modify the iovec; OutBuffers
	  // don't allow access to bytes behind the first unflushed offset.
	  const struct iovec & iov = buffers_[unflushedIndex];
	  // Offset within 'iov' of the first unflushed byte.
	  const size_t iovOffset = bufferUnflushedDataFirstOffset_ - unflushedOffset;
	  sassert(iovOffset < iov.iov_len);
	  if (owned_[unflushedIndex]) {
		// TODO(peb): if we create iovecs on demand,
		// we don't need this special handling.
		const ssize_t expectedBytes = iov.iov_len - iovOffset;
		const ssize_t bytesWritten = ::write(outputFileDescriptor, iov.iov_base + iovOffset, iov.iov_len - iovOffset);
		if (bytesWritten <= 0) {
		  if (bytesWritten == 0 || errno == EAGAIN) {
			return WRITE_AGAIN;
		  }
		  return WRITE_ERROR;
		}
		bufferUnflushedDataFirstOffset_ += bytesWritten;
		if (bytesWritten == expectedBytes) {
		  ++unflushedIndex;
		  unflushedOffset += iovec.iov_len;
		} // if bytesWritten < expectedBytes, we'll pick up from bufferUnflushedDataFirstOffset_.
	  } else {
		iov.iov_base += iovOffset;
		iov.iov_len -= iovOffset;
	  }
	}
	const ssize_t expectedBytes = size_ - unflushedOffset;
	const ssize_t bytesWritten = writev(outputFileDescriptor,
										&buffers_[unflushedIndex],
										buffers_.size() - unflushedIndex);
	if (bytesWritten <= 0) {
	  if (bytesWritten == 0 || errno == EAGAIN) {
		return WRITE_AGAIN;
	  }
	  return WRITE_ERROR;
	}
	bufferUnflushedDataFirstOffset_ += bytesWritten;
	if (bytesWritten == expectedBytes) {
	  return WRITE_DONE;
	}
	// bytesWritten < expectedBytes
	while (unflushedOffset + buffers_[unflushedIndex].iov_len < bufferUnflushedDataFirstOffset_) {
	  unflushedOffset += buffers_[unflushedIndex].iov_len;
	  ++unflushedIndex;
	}
  }
  return WRITE_DONE;
}

ssize_t mapIntoBuffer(const void *mem, size_t len){
  sassert(owned_.size() == buffers_.size());
  buffers_.emplace_back(mem, len);
  owned_.push_back(false);
  size_ += len;
}

size_t OutputBuffer::bytesInABuffer() const {
	return size_ - bufferUnflushedDataFirstOffset_;
}

ssize_t OutputBuffer::copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset) {
  buffers_.emplace_back(malloc(len), len);
  const struct iovec & iov = buffers_.back();
  sassert(iov.iov_base);
  owned_.emplace_back(true);
  off_t lOffset = offset != nullptr ? *offset : 0;
  size_t bytesRead = 0;
  while (len > 0) {
	ssize_t ret = pread(inputFileDescriptor, static_cast<uint8_t*>iov.iov_base + bytesRead, len, lOffset);
		if (ret <= 0) {
		  iov.iov_base = realloc(iov.iov_base, bytesRead);
		  eassert(iov.iov_base);
		  return bytesRead;
		}
		lOffset += ret;
		len -= ret;
		size_ += ret;
		bytesRead += ret;
	}
	return bytesRead;
}

ssize_t OutputBuffer::copyIntoBuffer(const void *mem, size_t len) {
  sassert(owned_.size() == buffers_.size());
  buffers_.emplace_back(malloc(len), len);
  const struct iovec & iov = buffers_.back();
  eassert(iov.iov_base);
  owned_.push_back(true);
  memcpy((void*)iov.iov_base, mem, len);
  size_ += len;
  return len;
}

OutputBuffer::~OutputBuffer() {
  sassert(owned_.size() == buffers_.size());
  for (int i = 0; i < owned_.size(); ++i) {
	if (owned_[i]) {
	  free(buffers_[i].iov_base);
	}
  }
}
