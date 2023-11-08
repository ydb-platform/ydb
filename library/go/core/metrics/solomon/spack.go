package solomon

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/ydb-platform/ydb/library/go/core/xerrors"
)

type spackVersion uint16

const (
	version11 spackVersion = 0x0101
	version12 spackVersion = 0x0102
)

type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) binaryWrite(data interface{}) {
	if ew.err != nil {
		return
	}
	switch t := data.(type) {
	case uint8:
		ew.err = binary.Write(ew.w, binary.LittleEndian, data.(uint8))
	case uint16:
		ew.err = binary.Write(ew.w, binary.LittleEndian, data.(uint16))
	case uint32:
		ew.err = binary.Write(ew.w, binary.LittleEndian, data.(uint32))
	default:
		ew.err = xerrors.Errorf("binaryWrite not supported type %v", t)
	}
}

func writeULEB128(w io.Writer, value uint32) error {
	remaining := value >> 7
	for remaining != 0 {
		err := binary.Write(w, binary.LittleEndian, uint8(value&0x7f|0x80))
		if err != nil {
			return xerrors.Errorf("binary.Write failed: %w", err)
		}
		value = remaining
		remaining >>= 7
	}
	err := binary.Write(w, binary.LittleEndian, uint8(value&0x7f))
	if err != nil {
		return xerrors.Errorf("binary.Write failed: %w", err)
	}
	return err
}

type spackMetric struct {
	flags uint8

	nameValueIndex uint32
	labelsCount    uint32
	labels         bytes.Buffer

	metric Metric
}

func (s *spackMetric) writeLabelPool(se *spackEncoder, namesIdx map[string]uint32, valuesIdx map[string]uint32, name string, value string) error {
	_, ok := namesIdx[name]
	if !ok {
		namesIdx[name] = se.nameCounter
		se.nameCounter++
		_, err := se.labelNamePool.WriteString(name)
		if err != nil {
			return err
		}
		err = se.labelNamePool.WriteByte(0)
		if err != nil {
			return err
		}
	}

	_, ok = valuesIdx[value]
	if !ok {
		valuesIdx[value] = se.valueCounter
		se.valueCounter++
		_, err := se.labelValuePool.WriteString(value)
		if err != nil {
			return err
		}
		err = se.labelValuePool.WriteByte(0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *spackMetric) writeLabel(se *spackEncoder, namesIdx map[string]uint32, valuesIdx map[string]uint32, name string, value string) error {
	s.labelsCount++

	err := s.writeLabelPool(se, namesIdx, valuesIdx, name, value)
	if err != nil {
		return err
	}

	err = writeULEB128(&s.labels, uint32(namesIdx[name]))
	if err != nil {
		return err
	}
	err = writeULEB128(&s.labels, uint32(valuesIdx[value]))
	if err != nil {
		return err
	}

	return nil
}

func (s *spackMetric) writeMetric(w io.Writer, version spackVersion) error {
	metricValueType := valueTypeOneWithoutTS
	if s.metric.getTimestamp() != nil {
		metricValueType = valueTypeOneWithTS
	}
	// library/cpp/monlib/encode/spack/spack_v1_encoder.cpp?rev=r9098142#L190
	types := uint8(s.metric.getType()<<2) | uint8(metricValueType)
	err := binary.Write(w, binary.LittleEndian, types)
	if err != nil {
		return xerrors.Errorf("binary.Write types failed: %w", err)
	}

	err = binary.Write(w, binary.LittleEndian, uint8(s.flags))
	if err != nil {
		return xerrors.Errorf("binary.Write flags failed: %w", err)
	}
	if version >= version12 {
		err = writeULEB128(w, uint32(s.nameValueIndex))
		if err != nil {
			return xerrors.Errorf("writeULEB128 name value index: %w", err)
		}
	}
	err = writeULEB128(w, uint32(s.labelsCount))
	if err != nil {
		return xerrors.Errorf("writeULEB128 labels count failed: %w", err)
	}

	_, err = w.Write(s.labels.Bytes()) // s.writeLabels(w)
	if err != nil {
		return xerrors.Errorf("write labels failed: %w", err)
	}
	if s.metric.getTimestamp() != nil {
		err = binary.Write(w, binary.LittleEndian, uint32(s.metric.getTimestamp().Unix()))
		if err != nil {
			return xerrors.Errorf("write timestamp failed: %w", err)
		}
	}

	switch s.metric.getType() {
	case typeGauge:
		err = binary.Write(w, binary.LittleEndian, s.metric.getValue().(float64))
		if err != nil {
			return xerrors.Errorf("binary.Write gauge value failed: %w", err)
		}
	case typeIGauge:
		err = binary.Write(w, binary.LittleEndian, s.metric.getValue().(int64))
		if err != nil {
			return xerrors.Errorf("binary.Write igauge value failed: %w", err)
		}
	case typeCounter, typeRated:
		err = binary.Write(w, binary.LittleEndian, uint64(s.metric.getValue().(int64)))
		if err != nil {
			return xerrors.Errorf("binary.Write counter value failed: %w", err)
		}
	case typeHistogram, typeRatedHistogram:
		h := s.metric.getValue().(histogram)
		err = h.writeHistogram(w)
		if err != nil {
			return xerrors.Errorf("writeHistogram failed: %w", err)
		}
	default:
		return xerrors.Errorf("unknown metric type: %v", s.metric.getType())
	}
	return nil
}

type SpackOpts func(*spackEncoder)

func WithVersion12() func(*spackEncoder) {
	return func(se *spackEncoder) {
		se.version = version12
	}
}

type spackEncoder struct {
	context     context.Context
	compression uint8
	version     spackVersion

	nameCounter  uint32
	valueCounter uint32

	labelNamePool  bytes.Buffer
	labelValuePool bytes.Buffer

	metrics Metrics
}

func NewSpackEncoder(ctx context.Context, compression CompressionType, metrics *Metrics, opts ...SpackOpts) *spackEncoder {
	if metrics == nil {
		metrics = &Metrics{}
	}
	se := &spackEncoder{
		context:     ctx,
		compression: uint8(compression),
		version:     version11,
		metrics:     *metrics,
	}
	for _, op := range opts {
		op(se)
	}
	return se
}

func (se *spackEncoder) writeLabels() ([]spackMetric, error) {
	namesIdx := make(map[string]uint32)
	valuesIdx := make(map[string]uint32)
	spackMetrics := make([]spackMetric, len(se.metrics.metrics))

	for idx, metric := range se.metrics.metrics {
		m := spackMetric{metric: metric}

		var err error
		if se.version >= version12 {
			err = m.writeLabelPool(se, namesIdx, valuesIdx, metric.getNameTag(), metric.Name())
			m.nameValueIndex = valuesIdx[metric.getNameTag()]
		} else {
			err = m.writeLabel(se, namesIdx, valuesIdx, metric.getNameTag(), metric.Name())
		}
		if err != nil {
			return nil, err
		}

		for name, value := range metric.getLabels() {
			if err := m.writeLabel(se, namesIdx, valuesIdx, name, value); err != nil {
				return nil, err
			}

		}
		spackMetrics[idx] = m
	}

	return spackMetrics, nil
}

func (se *spackEncoder) Encode(w io.Writer) (written int, err error) {
	spackMetrics, err := se.writeLabels()
	if err != nil {
		return written, xerrors.Errorf("writeLabels failed: %w", err)
	}

	err = se.writeHeader(w)
	if err != nil {
		return written, xerrors.Errorf("writeHeader failed: %w", err)
	}
	written += HeaderSize
	compression := CompressionType(se.compression)

	cw := newCompressedWriter(w, compression)

	err = se.writeLabelNamesPool(cw)
	if err != nil {
		return written, xerrors.Errorf("writeLabelNamesPool failed: %w", err)
	}

	err = se.writeLabelValuesPool(cw)
	if err != nil {
		return written, xerrors.Errorf("writeLabelValuesPool failed: %w", err)
	}

	err = se.writeCommonTime(cw)
	if err != nil {
		return written, xerrors.Errorf("writeCommonTime failed: %w", err)
	}

	err = se.writeCommonLabels(cw)
	if err != nil {
		return written, xerrors.Errorf("writeCommonLabels failed: %w", err)
	}

	err = se.writeMetricsData(cw, spackMetrics)
	if err != nil {
		return written, xerrors.Errorf("writeMetricsData failed: %w", err)
	}

	err = cw.Close()
	if err != nil {
		return written, xerrors.Errorf("close failed: %w", err)
	}

	switch compression {
	case CompressionNone:
		written += cw.(*noCompressionWriteCloser).written
	case CompressionLz4:
		written += cw.(*lz4CompressionWriteCloser).written
	}

	return written, nil
}

func (se *spackEncoder) writeHeader(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}
	ew := &errWriter{w: w}
	ew.binaryWrite(uint16(0x5053))                  // Magic
	ew.binaryWrite(uint16(se.version))              // Version
	ew.binaryWrite(uint16(24))                      // HeaderSize
	ew.binaryWrite(uint8(0))                        // TimePrecision(SECONDS)
	ew.binaryWrite(uint8(se.compression))           // CompressionAlg
	ew.binaryWrite(uint32(se.labelNamePool.Len()))  // LabelNamesSize
	ew.binaryWrite(uint32(se.labelValuePool.Len())) // LabelValuesSize
	ew.binaryWrite(uint32(len(se.metrics.metrics))) // MetricsCount
	ew.binaryWrite(uint32(len(se.metrics.metrics))) // PointsCount
	if ew.err != nil {
		return xerrors.Errorf("binaryWrite failed: %w", ew.err)
	}
	return nil
}

func (se *spackEncoder) writeLabelNamesPool(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}
	_, err := w.Write(se.labelNamePool.Bytes())
	if err != nil {
		return xerrors.Errorf("write labelNamePool failed: %w", err)
	}
	return nil
}

func (se *spackEncoder) writeLabelValuesPool(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}

	_, err := w.Write(se.labelValuePool.Bytes())
	if err != nil {
		return xerrors.Errorf("write labelValuePool failed: %w", err)
	}
	return nil
}

func (se *spackEncoder) writeCommonTime(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}

	if se.metrics.timestamp == nil {
		return binary.Write(w, binary.LittleEndian, uint32(0))
	}
	return binary.Write(w, binary.LittleEndian, uint32(se.metrics.timestamp.Unix()))
}

func (se *spackEncoder) writeCommonLabels(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}

	_, err := w.Write([]byte{0})
	if err != nil {
		return xerrors.Errorf("write commonLabels failed: %w", err)
	}
	return nil
}

func (se *spackEncoder) writeMetricsData(w io.Writer, metrics []spackMetric) error {
	for _, s := range metrics {
		if se.context.Err() != nil {
			return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
		}

		err := s.writeMetric(w, se.version)
		if err != nil {
			return xerrors.Errorf("write metric failed: %w", err)
		}
	}
	return nil
}
