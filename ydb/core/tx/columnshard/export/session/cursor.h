#pragma once
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimrColumnShardExportProto {
class TCursor;
}

namespace NKikimr::NOlap::NExport {

class TCursor {
private:
    ui32 ChunkIdx = 1;
    std::optional<TOwnedCellVec> LastKey;
    bool Finished = false;

    std::optional<bool> MultiFile;
    ui32 FileIdx = 0;
    std::optional<TOwnedCellVec> FileStartKey;
    ui64 FileRows = 0;
    bool DataFinished = false;

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrColumnShardExportProto::TCursor &proto);

public:
    TCursor() = default;
    TCursor(const TOwnedCellVec &lastKey, const bool finished);

    const std::optional<TOwnedCellVec> &GetLastKey() const;

    ui32 GetChunkIdx() const;

    bool HasLastKey() const;

    bool IsFinished() const;

    void InitNext(const TOwnedCellVec &lastKey, const bool finished);

    bool HasMultiFile() const {
        return MultiFile.has_value();
    }

    void SetMultiFile(const bool value) {
        MultiFile = value;
    }

    bool IsMultiFile() const {
        return MultiFile.value_or(false);
    }

    ui32 GetFileIdx() const {
        return FileIdx;
    }

    const std::optional<TOwnedCellVec> &GetFileStartKey() const {
        return FileStartKey;
    }

    ui64 GetFileRows() const {
        return FileRows;
    }

    void AddFileRows(const ui64 rows) {
        FileRows += rows;
    }

    bool IsDataFinished() const {
        return DataFinished;
    }

    // Restart the data file being written from its beginning
    void RewindToFileStart();
    // The data file being written is uploaded and ends with lastKey, the next one starts after it
    void StartNextFile(const TOwnedCellVec &lastKey);
    // The data file being written is uploaded and it is the last one
    void FinishData();
    // The data file being written has no rows and is not uploaded: the previous file is the last one
    void FinishDataWithoutCurrentFile();

    static TConclusion<TCursor> BuildFromProto(const NKikimrColumnShardExportProto::TCursor &proto);

    NKikimrColumnShardExportProto::TCursor SerializeToProto() const;
};

}   // namespace NKikimr::NOlap::NExport
