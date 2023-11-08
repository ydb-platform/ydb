package slices

func Chunk[T any](slice []T, chunkSize int) [][]T {
	if chunkSize < 1 {
		return [][]T{slice}
	}
	chunksCount := len(slice) / chunkSize
	if len(slice)%chunkSize > 0 {
		chunksCount++
	}
	chunks := make([][]T, chunksCount)

	for i := range chunks {
		if len(slice) < chunkSize {
			chunkSize = len(slice)
		}
		chunks[i] = slice[0:chunkSize]
		slice = slice[chunkSize:]
	}
	return chunks
}
