/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef SPDK_DIF_H
#define SPDK_DIF_H

#include "spdk/stdinc.h"
#include "spdk/assert.h"

#define SPDK_DIF_FLAGS_REFTAG_CHECK	(1U << 26)
#define SPDK_DIF_FLAGS_APPTAG_CHECK	(1U << 27)
#define SPDK_DIF_FLAGS_GUARD_CHECK	(1U << 28)

#define SPDK_DIF_REFTAG_ERROR	0x1
#define SPDK_DIF_APPTAG_ERROR	0x2
#define SPDK_DIF_GUARD_ERROR	0x4
#define SPDK_DIF_DATA_ERROR	0x8

enum spdk_dif_type {
	SPDK_DIF_DISABLE = 0,
	SPDK_DIF_TYPE1 = 1,
	SPDK_DIF_TYPE2 = 2,
	SPDK_DIF_TYPE3 = 3,
};

enum spdk_dif_check_type {
	SPDK_DIF_CHECK_TYPE_REFTAG = 1,
	SPDK_DIF_CHECK_TYPE_APPTAG = 2,
	SPDK_DIF_CHECK_TYPE_GUARD = 3,
};

struct spdk_dif {
	uint16_t guard;
	uint16_t app_tag;
	uint32_t ref_tag;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_dif) == 8, "Incorrect size");

/** DIF context information */
struct spdk_dif_ctx {
	/** Block size */
	uint32_t		block_size;

	/** Metadata size */
	uint32_t		md_size;

	/** Metadata location */
	bool			md_interleave;

	/** Interval for guard computation for DIF */
	uint32_t		guard_interval;

	/** DIF type */
	enum spdk_dif_type	dif_type;

	/* Flags to specify the DIF action */
	uint32_t		dif_flags;

	/* Initial reference tag */
	uint32_t		init_ref_tag;

	/** Application tag */
	uint16_t		app_tag;

	/* Application tag mask */
	uint16_t		apptag_mask;

	/* Byte offset from the start of the whole data buffer. */
	uint32_t		data_offset;

	/* Offset to initial reference tag */
	uint32_t		ref_tag_offset;

	/** Guard value of the last data block.
	 *
	 * Interim guard value is set if the last data block is partial, or
	 * seed value is set otherwise.
	 */
	uint16_t		last_guard;

	/* Seed value for guard computation */
	uint16_t		guard_seed;

	/* Remapped initial reference tag. */
	uint32_t		remapped_init_ref_tag;
};

/** DIF error information */
struct spdk_dif_error {
	/** Error type */
	uint8_t		err_type;

	/** Expected value */
	uint32_t	expected;

	/** Actual value */
	uint32_t	actual;

	/** Offset the error occurred at, block based */
	uint32_t	err_offset;
};

/**
 * Initialize DIF context.
 *
 * \param ctx DIF context.
 * \param block_size Block size in a block.
 * \param md_size Metadata size in a block.
 * \param md_interleave If true, metadata is interleaved with block data.
 * If false, metadata is separated with block data.
 * \param dif_loc DIF location. If true, DIF is set in the first 8 bytes of metadata.
 * If false, DIF is in the last 8 bytes of metadata.
 * \param dif_type Type of DIF.
 * \param dif_flags Flag to specify the DIF action.
 * \param init_ref_tag Initial reference tag. For type 1, this is the
 * starting block address.
 * \param apptag_mask Application tag mask.
 * \param app_tag Application tag.
 * \param data_offset Byte offset from the start of the whole data buffer.
 * \param guard_seed Seed value for guard computation.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_ctx_init(struct spdk_dif_ctx *ctx, uint32_t block_size, uint32_t md_size,
		      bool md_interleave, bool dif_loc, enum spdk_dif_type dif_type, uint32_t dif_flags,
		      uint32_t init_ref_tag, uint16_t apptag_mask, uint16_t app_tag,
		      uint32_t data_offset, uint16_t guard_seed);

/**
 * Update date offset of DIF context.
 *
 * \param ctx DIF context.
 * \param data_offset Byte offset from the start of the whole data buffer.
 */
void spdk_dif_ctx_set_data_offset(struct spdk_dif_ctx *ctx, uint32_t data_offset);

/**
 * Set remapped initial reference tag of DIF context.
 *
 * \param ctx DIF context.
 * \param remapped_init_ref_tag Remapped initial reference tag. For type 1, this is the
 * starting block address.
 */
void spdk_dif_ctx_set_remapped_init_ref_tag(struct spdk_dif_ctx *ctx,
		uint32_t remapped_init_ref_tag);

/**
 * Generate DIF for extended LBA payload.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param num_blocks Number of blocks of the payload.
 * \param ctx DIF context.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_generate(struct iovec *iovs, int iovcnt, uint32_t num_blocks,
		      const struct spdk_dif_ctx *ctx);

/**
 * Verify DIF for extended LBA payload.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param num_blocks Number of blocks of the payload.
 * \param ctx DIF context.
 * \param err_blk Error information of the block in which DIF error is found.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_verify(struct iovec *iovs, int iovcnt, uint32_t num_blocks,
		    const struct spdk_dif_ctx *ctx, struct spdk_dif_error *err_blk);

/**
 * Calculate CRC-32C checksum for extended LBA payload.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param num_blocks Number of blocks of the payload.
 * \param crc32c Initial and updated CRC-32C value.
 * \param ctx DIF context.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_update_crc32c(struct iovec *iovs, int iovcnt, uint32_t num_blocks,
			   uint32_t *crc32c, const struct spdk_dif_ctx *ctx);

/**
 * Copy data and generate DIF for extended LBA payload.
 *
 * \param iovs iovec array describing the LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param bounce_iov A contiguous buffer forming extended LBA payload.
 * \param num_blocks Number of blocks of the LBA payload.
 * \param ctx DIF context.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_generate_copy(struct iovec *iovs, int iovcnt, struct iovec *bounce_iov,
			   uint32_t num_blocks, const struct spdk_dif_ctx *ctx);

/**
 * Verify DIF and copy data for extended LBA payload.
 *
 * \param iovs iovec array describing the LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param bounce_iov A contiguous buffer forming extended LBA payload.
 * \param num_blocks Number of blocks of the LBA payload.
 * \param ctx DIF context.
 * \param err_blk Error information of the block in which DIF error is found.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_verify_copy(struct iovec *iovs, int iovcnt, struct iovec *bounce_iov,
			 uint32_t num_blocks, const struct spdk_dif_ctx *ctx,
			 struct spdk_dif_error *err_blk);

/**
 * Inject bit flip error to extended LBA payload.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param num_blocks Number of blocks of the payload.
 * \param ctx DIF context.
 * \param inject_flags Flags to specify the action of error injection.
 * \param inject_offset Offset, in blocks, to which error is injected.
 * If multiple error is injected, only the last injection is stored.
 *
 * \return 0 on success and negated errno otherwise including no metadata.
 */
int spdk_dif_inject_error(struct iovec *iovs, int iovcnt, uint32_t num_blocks,
			  const struct spdk_dif_ctx *ctx, uint32_t inject_flags,
			  uint32_t *inject_offset);

/**
 * Generate DIF for separate metadata payload.
 *
 * \param iovs iovec array describing the LBA payload.
 * \params iovcnt Number of elements in iovs.
 * \param md_iov A contiguous buffer for metadata.
 * \param num_blocks Number of blocks of the separate metadata payload.
 * \param ctx DIF context.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dix_generate(struct iovec *iovs, int iovcnt, struct iovec *md_iov,
		      uint32_t num_blocks, const struct spdk_dif_ctx *ctx);

/**
 * Verify DIF for separate metadata payload.
 *
 * \param iovs iovec array describing the LBA payload.
 * \params iovcnt Number of elements in iovs.
 * \param md_iov A contiguous buffer for metadata.
 * \param num_blocks Number of blocks of the separate metadata payload.
 * \param ctx DIF context.
 * \param err_blk Error information of the block in which DIF error is found.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dix_verify(struct iovec *iovs, int iovcnt, struct iovec *md_iov,
		    uint32_t num_blocks, const struct spdk_dif_ctx *ctx,
		    struct spdk_dif_error *err_blk);

/**
 * Inject bit flip error to separate metadata payload.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param md_iov A contiguous buffer for metadata.
 * \param num_blocks Number of blocks of the payload.
 * \param ctx DIF context.
 * \param inject_flags Flag to specify the action of error injection.
 * \param inject_offset Offset, in blocks, to which error is injected.
 * If multiple error is injected, only the last injection is stored.
 *
 * \return 0 on success and negated errno otherwise including no metadata.
 */
int spdk_dix_inject_error(struct iovec *iovs, int iovcnt, struct iovec *md_iov,
			  uint32_t num_blocks, const struct spdk_dif_ctx *ctx,
			  uint32_t inject_flags, uint32_t *inject_offset);

/**
 * Setup iovec array to leave a space for metadata for each block.
 *
 * This function is used to leave a space for metadata for each block when
 * the network socket reads data, or to make the network socket ignore a
 * space for metadata for each block when the network socket writes data.
 * This function removes the necessity of data copy in the SPDK application
 * during DIF insertion and strip.
 *
 * When the extended LBA payload is splitted into multiple data segments,
 * start of each data segment is passed through the DIF context. data_offset
 * and data_len is within a data segment.
 *
 * \param iovs iovec array set by this function.
 * \param iovcnt Number of elements in the iovec array.
 * \param buf_iovs SGL for the buffer to create extended LBA payload.
 * \param buf_iovcnt Size of the SGL for the buffer to create extended LBA payload.
 * \param data_offset Offset to store the next incoming data in the current data segment.
 * \param data_len Expected length of the newly read data in the current data segment of
 * the extended LBA payload.
 * \param mapped_len Output parameter that will contain data length mapped by
 * the iovec array.
 * \param ctx DIF context.
 *
 * \return Number of used elements in the iovec array on success or negated
 * errno otherwise.
 */
int spdk_dif_set_md_interleave_iovs(struct iovec *iovs, int iovcnt,
				    struct iovec *buf_iovs, int buf_iovcnt,
				    uint32_t data_offset, uint32_t data_len,
				    uint32_t *mapped_len,
				    const struct spdk_dif_ctx *ctx);

/**
 * Generate and insert DIF into metadata space for newly read data block.
 *
 * When the extended LBA payload is splitted into multiple data segments,
 * start of each data segment is passed through the DIF context. data_offset
 * and data_len is within a data segment.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param data_offset Offset to the newly read data in the current data segment of
 * the extended LBA payload.
 * \param data_len Length of the newly read data in the current data segment of
 * the extended LBA payload.
 * \param ctx DIF context.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_generate_stream(struct iovec *iovs, int iovcnt,
			     uint32_t data_offset, uint32_t data_len,
			     struct spdk_dif_ctx *ctx);

/**
 * Verify DIF for the to-be-written block of the extended LBA payload.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param data_offset Offset to the to-be-written data in the extended LBA payload.
 * \param data_len Length of the to-be-written data in the extended LBA payload.
 * \param ctx DIF context.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_verify_stream(struct iovec *iovs, int iovcnt,
			   uint32_t data_offset, uint32_t data_len,
			   struct spdk_dif_ctx *ctx,
			   struct spdk_dif_error *err_blk);

/**
 * Calculate CRC-32C checksum of the specified range in the extended LBA payload.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param data_offset Offset to the range
 * \param data_len Length of the range
 * \param crc32c Initial and updated CRC-32C value.
 * \param ctx DIF context.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_update_crc32c_stream(struct iovec *iovs, int iovcnt,
				  uint32_t data_offset, uint32_t data_len,
				  uint32_t *crc32c, const struct spdk_dif_ctx *ctx);
/**
 * Convert offset and size from LBA based to extended LBA based.
 *
 * \param data_offset Data offset
 * \param data_len Data length
 * \param buf_offset Buffer offset converted from data offset.
 * \param buf_len Buffer length converted from data length
 * \param ctx DIF context.
 */
void spdk_dif_get_range_with_md(uint32_t data_offset, uint32_t data_len,
				uint32_t *buf_offset, uint32_t *buf_len,
				const struct spdk_dif_ctx *ctx);

/**
 * Convert length from LBA based to extended LBA based.
 *
 * \param data_len Data length
 * \param ctx DIF context.
 *
 * \return Extended LBA based data length.
 */
uint32_t spdk_dif_get_length_with_md(uint32_t data_len, const struct spdk_dif_ctx *ctx);

/**
 * Remap reference tag for extended LBA payload.
 *
 * When using stacked virtual bdev (e.g. split virtual bdev), block address space for I/O
 * will be remapped during I/O processing and so reference tag will have to be remapped
 * accordingly. This patch is for that case.
 *
 * \param iovs iovec array describing the extended LBA payload.
 * \param iovcnt Number of elements in the iovec array.
 * \param num_blocks Number of blocks of the payload.
 * \param ctx DIF context.
 * \param err_blk Error information of the block in which DIF error is found.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dif_remap_ref_tag(struct iovec *iovs, int iovcnt, uint32_t num_blocks,
			   const struct spdk_dif_ctx *dif_ctx,
			   struct spdk_dif_error *err_blk);

/**
 * Remap reference tag for separate metadata payload.
 *
 * When using stacked virtual bdev (e.g. split virtual bdev), block address space for I/O
 * will be remapped during I/O processing and so reference tag will have to be remapped
 * accordingly. This patch is for that case.
 *
 * \param md_iov A contiguous buffer for metadata.
 * \param num_blocks Number of blocks of the payload.
 * \param ctx DIF context.
 * \param err_blk Error information of the block in which DIF error is found.
 *
 * \return 0 on success and negated errno otherwise.
 */
int spdk_dix_remap_ref_tag(struct iovec *md_iov, uint32_t num_blocks,
			   const struct spdk_dif_ctx *dif_ctx,
			   struct spdk_dif_error *err_blk);
#endif /* SPDK_DIF_H */
