namespace antlr3 {

template<class ImplTraits>
ANTLR_FDSC	FileUtils<ImplTraits>::AntlrFopen(const ANTLR_UINT8* filename, const char * mode)
{
	return  (ANTLR_FDSC)fopen((const char *)filename, mode);
}

template<class ImplTraits>
void	FileUtils<ImplTraits>::AntlrFclose	(ANTLR_FDSC fd)
{
	fclose(fd);
}

template<class ImplTraits>
ANTLR_UINT32	FileUtils<ImplTraits>::AntlrFsize(const ANTLR_UINT8* filename)
{
    struct _stat	statbuf;

    _stat((const char *)filename, &statbuf);

    return (ANTLR_UINT32)statbuf.st_size;
}

template<class ImplTraits>
ANTLR_UINT32	FileUtils<ImplTraits>::AntlrFread(ANTLR_FDSC fdsc, ANTLR_UINT32 count,  void* data)
{
	return  (ANTLR_UINT32)fread(data, (size_t)count, 1, fdsc);
}

template<class ImplTraits>
	template<typename InputStreamType>
ANTLR_UINT32	FileUtils<ImplTraits>::AntlrRead8Bit(InputStreamType* input, const ANTLR_UINT8* fileName)
{
	ANTLR_FDSC	    infile;
	ANTLR_UINT32	    fSize;

	/* Open the OS file in read binary mode
	*/
	infile  = FileUtils<ImplTraits>::AntlrFopen(fileName, "rb");

	/* Check that it was there
	*/
	if	(infile == NULL)
	{
		ParseFileAbsentException ex;
		throw ex;
	}

	/* It was there, so we can read the bytes now
	*/
	fSize   = FileUtils<ImplTraits>::AntlrFsize(fileName);	/* Size of input file	*/

	/* Allocate buffer for this input set   
	*/
	void* data = ImplTraits::AllocPolicyType::alloc(fSize);
	/* Now we read the file. Characters are not converted to
	* the internal ANTLR encoding until they are read from the buffer
	*/
	FileUtils<ImplTraits>::AntlrFread(infile, fSize, data );

	input->set_data( (unsigned char*) data );
	input->set_sizeBuf( fSize );

	input->set_isAllocated(true);

	/* And close the file handle
	*/
	FileUtils<ImplTraits>::AntlrFclose(infile);

	return  ANTLR_SUCCESS;
}

}
