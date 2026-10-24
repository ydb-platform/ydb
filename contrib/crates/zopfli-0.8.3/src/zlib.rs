use crate::{BlockType, DeflateEncoder, Error, Options, Write};

/// A Zlib encoder powered by the Zopfli algorithm, that compresses data using
/// a [`DeflateEncoder`]. Most users will find using [`compress`](crate::compress)
/// easier and more performant.
///
/// The caveats about short writes in [`DeflateEncoder`]s carry over to `ZlibEncoder`s:
/// for best performance and compression, it is best to avoid them. One way to ensure
/// this is to use the [`new_buffered`](ZlibEncoder::new_buffered) method.
pub struct ZlibEncoder<W: Write> {
    deflate_encoder: Option<DeflateEncoder<W>>,
    adler_hasher: simd_adler32::Adler32,
}

impl<W: Write> ZlibEncoder<W> {
    /// Creates a new Zlib encoder that will operate according to the
    /// specified options.
    pub fn new(options: Options, btype: BlockType, mut sink: W) -> Result<Self, Error> {
        let cmf = 120; // CM 8, CINFO 7. See zlib spec.
        let flevel = 3;
        let fdict = 0;
        let mut cmfflg: u16 = 256 * cmf + fdict * 32 + flevel * 64;
        let fcheck = 31 - cmfflg % 31;
        cmfflg += fcheck;

        sink.write_all(&cmfflg.to_be_bytes())?;

        Ok(Self {
            deflate_encoder: Some(DeflateEncoder::new(options, btype, sink)),
            adler_hasher: simd_adler32::Adler32::new(),
        })
    }

    /// Creates a new Zlib encoder that operates according to the specified
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
    /// will have received a complete Zlib stream when this method
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

        sink.write_all(&self.adler_hasher.finish().to_be_bytes())?;

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

impl<W: Write> Write for ZlibEncoder<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.deflate_encoder
            .as_mut()
            .unwrap()
            .write(buf)
            .map(|bytes_written| {
                self.adler_hasher.write(&buf[..bytes_written]);
                bytes_written
            })
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.deflate_encoder.as_mut().unwrap().flush()
    }
}

impl<W: Write> Drop for ZlibEncoder<W> {
    fn drop(&mut self) {
        self.__finish().ok();
    }
}

// Boilerplate to make latest Rustdoc happy: https://github.com/rust-lang/rust/issues/117796
#[cfg(all(doc, feature = "std"))]
impl<W: crate::io::Write> std::io::Write for ZlibEncoder<W> {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        unimplemented!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!()
    }
}
