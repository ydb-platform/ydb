namespace antlr3 {

//static 
ANTLR_INLINE void GenericStream::displayRecognitionError( const StringType& str )
{
	fprintf(stderr, str.c_str() );
}

}
