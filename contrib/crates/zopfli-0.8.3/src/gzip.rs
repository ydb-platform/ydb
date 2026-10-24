use crate::{BlockType, DeflateEncoder, Error, Options, Write};

/// A Gzip encoder powered by the Zopfli algorithm, that compresses data using
/// a [`DeflateEncoder`]. Most users will find using [`compress`](crate::compress)
/// easier and more performant.
///
/// The caveats about short writes in [`DeflateEncoder`]s carry over to `GzipEncoder`s:
/// for best performance and compression, it is best to avoid them. One way to ensure
/// this is to use the [`new_buffered`](GzipEncoder::new_buffered) method.
pub struct GzipEncoder<W: Write> {
    deflate_encoder: Option<DeflateEncoder<W>>,
    crc32_hasher: crc32fast::Hasher,
    input_size: u32,
}

impl<W: Write> GzipEncoder<W> {
    /// Creates a new Gzip encoder that will operate according to the
    /// specified options.
    pub fn new(options: Options, btype: BlockType, mut sink: W) -> Result<Self, Error> {
        static HEADER: &[u8] = &[
            31,  // ID1
            139, // ID2
            8,   // CM
            0,   // FLG
            0,   // MTIME
            0, 0, 0, 2, // XFL, 2 indicates best compression.
            3, // OS follows Unix conventions.
        ];

        sink.write_all(HEADER)?;

        Ok(Self {
            deflate_encoder: Some(DeflateEncoder::new(options, btype, sink)),
            crc32_hasher: crc32fast::Hasher::new(),
            input_size: 0,
        })
    }

    /// Creates a new Gzip encoder that operates according to the specified
    /// options and is wrapped with a buffer to guarantee that data is
    /// compressed in large chunks, which is necessary for decent performance
    /// and good compression ratio.
    #[cfg(feature = "std")]
    pub fn new_buffered(
        options: Options,
        btype: BlockType,
        sink: W,
    ) -> Result<std::io::BufWriter<Self>, Error> {
        Ok(std::io::BufWriter::with_capacity(
            crate::util::ZOPFLI_MASTER_BLOCK_SIZE,
            Self::new(options, btype, sink)?,
        ))
    }

    /// Encodes any pending chunks of data and writes them to the sink,
    /// consuming the encoder and returning the wrapped sink. The sink
    /// will have received a complete Gzip stream when this method
    /// returns.
    ///
    /// The encoder is automatically [`finish`](Self::finish)ed when
    /// dropped, but explicitly finishing it with this method allows
    /// handling I/O errors.
    pub fn finish(mut self) -> Result<W, Error> {
        self.__finish().map(|sink| sink.unwrap())
    }

    fn __finish(&mut self) -> Result<Option<W>, Error> {
        if self.deflate_encoder.is_none() {
            return Ok(None);
        }

        let mut sink = self.deflate_encoder.take().unwrap().finish()?;

        sink.write_all(&self.crc32_hasher.clone().finalize().to_le_bytes())?;
        sink.write_all(&self.input_size.to_le_bytes())?;

        Ok(Some(sink))
    }

    /// Gets a reference to the underlying writer.
    pub fn get_ref(&self) -> &W {
        self.deflate_encoder.as_ref().unwrap().get_ref()
    }

    /// Gets a mutable reference to the underlying writer.
    ///
    /// Note that mutating the output/input state of the stream may corrupt
    /// this object, so care must be taken when using this method.
    pub fn get_mut(&mut self) -> &mut W {
        self.deflate_encoder.as_mut().unwrap().get_mut()
    }
}

impl<W: Write> Write for GzipEncoder<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.deflate_encoder
            .as_mut()
            .unwrap()
            .write(buf)
            .map(|bytes_written| {
                self.crc32_hasher.update(&buf[..bytes_written]);
                self.input_size = self.input_size.wrapping_add(bytes_written as u32);
                bytes_written
            })
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.deflate_encoder.as_mut().unwrap().flush()
    }
}

impl<W: Write> Drop for GzipEncoder<W> {
    fn drop(&mut self) {
        self.__finish().ok();
    }
}

// Boilerplate to make latest Rustdoc happy: https://github.com/rust-lang/rust/issues/117796
#[cfg(all(doc, feature = "std"))]
impl<W: crate::io::Write> std::io::Write for GzipEncoder<W> {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        unimplemented!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!()
    }
}
