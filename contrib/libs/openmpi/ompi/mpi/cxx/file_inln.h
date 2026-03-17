// -*- c++ -*-
//
// Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
//                         University Research and Technology
//                         Corporation.  All rights reserved.
// Copyright (c) 2004-2005 The University of Tennessee and The University
//                         of Tennessee Research Foundation.  All rights
//                         reserved.
// Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
//                         University of Stuttgart.  All rights reserved.
// Copyright (c) 2004-2005 The Regents of the University of California.
//                         All rights reserved.
// Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


inline void
MPI::File::Delete(const char* filename, const MPI::Info& info)
{
  (void) MPI_File_delete(const_cast<char *>(filename), info);
}


inline int
MPI::File::Get_amode() const
{
  int amode;
  (void) MPI_File_get_amode(mpi_file, &amode);
  return amode;
}


inline bool
MPI::File::Get_atomicity() const
{
  int flag;
  (void) MPI_File_get_atomicity(mpi_file, &flag);
  return OPAL_INT_TO_BOOL(flag);
}

inline MPI::Offset
MPI::File::Get_byte_offset(const MPI::Offset disp) const
{
  MPI_Offset offset, ldisp;
  ldisp = disp;
  (void) MPI_File_get_byte_offset(mpi_file, ldisp, &offset);
  return offset;
}

inline MPI::Group
MPI::File::Get_group() const
{
  MPI_Group group;
  (void) MPI_File_get_group(mpi_file, &group);
  return group;
}


inline MPI::Info
MPI::File::Get_info() const
{
  MPI_Info info_used;
  (void) MPI_File_get_info(mpi_file, &info_used);
  return info_used;
}


inline MPI::Offset
MPI::File::Get_position() const
{
  MPI_Offset offset;
  (void) MPI_File_get_position(mpi_file, &offset);
  return offset;
}


inline MPI::Offset
MPI::File::Get_position_shared() const
{
  MPI_Offset offset;
  (void) MPI_File_get_position_shared(mpi_file, &offset);
  return offset;
}


inline MPI::Offset
MPI::File::Get_size() const
{
  MPI_Offset offset;
  (void) MPI_File_get_size(mpi_file, &offset);
  return offset;

}


inline MPI::Aint
MPI::File::Get_type_extent(const MPI::Datatype& datatype) const
{
  MPI_Aint extent;
  (void) MPI_File_get_type_extent(mpi_file, datatype, &extent);
  return extent;
}


inline void
MPI::File::Get_view(MPI::Offset& disp,
			   MPI::Datatype& etype,
			   MPI::Datatype& filetype,
			   char* datarep) const
{
  MPI_Datatype type, ftype;
  type = etype;
  ftype = filetype;
  MPI::Offset odisp = disp;

  (void) MPI_File_get_view(mpi_file, &odisp, &type, &ftype,
			   datarep);
}


inline MPI::Request
MPI::File::Iread(void* buf, int count,
			      const MPI::Datatype& datatype)
{
  MPI_Request req;
  (void) MPI_File_iread(mpi_file, buf, count, datatype, &req);
  return req;
}


inline MPI::Request
MPI::File::Iread_at(MPI::Offset offset, void* buf, int count,
				 const MPI::Datatype& datatype)
{
  MPI_Request req;
  (void) MPI_File_iread_at(mpi_file, offset, buf, count, datatype, &req);
  return req;
}


inline MPI::Request
MPI::File::Iread_shared(void* buf, int count,
				     const MPI::Datatype& datatype)
{
  MPI_Request req;
  (void) MPI_File_iread_shared(mpi_file, buf, count, datatype, &req);
  return req;
}


inline MPI::Request
MPI::File::Iwrite(const void* buf, int count,
			 const MPI::Datatype& datatype)
{
  MPI_Request req;
  (void) MPI_File_iwrite(mpi_file, const_cast<void *>(buf), count, datatype, &req);
  return req;
}


inline MPI::Request
MPI::File::Iwrite_at(MPI::Offset offset, const void* buf,
			    int count, const MPI::Datatype& datatype)
{
  MPI_Request req;
  (void) MPI_File_iwrite_at(mpi_file, offset, const_cast<void *>(buf), count, datatype,
			    &req);
  return req;
}


inline MPI::Request
MPI::File::Iwrite_shared(const void* buf, int count,
				const MPI::Datatype& datatype)
{
  MPI_Request req;
  (void) MPI_File_iwrite_shared(mpi_file, const_cast<void *>(buf), count, datatype, &req);
  return req;
}


inline MPI::File
MPI::File::Open(const MPI::Intracomm& comm,
				 const char* filename, int amode,
				 const MPI::Info& info)
{
  MPI_File fh;
  (void) MPI_File_open(comm, const_cast<char *>(filename), amode, info, &fh);
  return fh;
}


inline void
MPI::File::Preallocate(MPI::Offset size)
{
  (void) MPI_File_preallocate(mpi_file, size);
}


inline void
MPI::File::Read(void* buf, int count,
		       const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_read(mpi_file, buf, count, datatype, &status);
}


inline void
MPI::File::Read(void* buf, int count,
		       const MPI::Datatype& datatype,
		       MPI::Status& status)
{
  (void) MPI_File_read(mpi_file, buf, count, datatype, &status.mpi_status);
}


inline void
MPI::File::Read_all(void* buf, int count,
			   const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_read_all(mpi_file, buf, count, datatype, &status);
}


inline void
MPI::File::Read_all(void* buf, int count,
			   const MPI::Datatype& datatype,
			   MPI::Status& status)
{
  (void) MPI_File_read_all(mpi_file, buf, count, datatype, &status.mpi_status);
}


inline void
MPI::File::Read_all_begin(void* buf, int count,
				 const MPI::Datatype& datatype)
{
  (void) MPI_File_read_all_begin(mpi_file, buf, count, datatype);
}


inline void
MPI::File::Read_all_end(void* buf)
{
  MPI_Status status;
  (void) MPI_File_read_all_end(mpi_file, buf, &status);
}


inline void
MPI::File::Read_all_end(void* buf, MPI::Status& status)
{
  (void) MPI_File_read_all_end(mpi_file, buf, &status.mpi_status);
}


inline void
MPI::File::Read_at(MPI::Offset offset,
			  void* buf, int count,
			  const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_read_at(mpi_file, offset, buf, count, datatype, &status);

}


inline void
MPI::File::Read_at(MPI::Offset offset, void* buf, int count,
			  const MPI::Datatype& datatype,
			  MPI::Status& status)
{
  (void) MPI_File_read_at(mpi_file, offset, buf, count, datatype,
                          &status.mpi_status);
}


inline void
MPI::File::Read_at_all(MPI::Offset offset, void* buf, int count,
			      const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_read_at_all(mpi_file, offset, buf, count, datatype, &status);
}


inline void
MPI::File::Read_at_all(MPI::Offset offset, void* buf, int count,
			      const MPI::Datatype& datatype,
			      MPI::Status& status)
{
  (void) MPI_File_read_at_all(mpi_file, offset, buf, count, datatype,
                              &status.mpi_status);
}


inline void
MPI::File::Read_at_all_begin(MPI::Offset offset,
				    void* buf, int count,
				    const MPI::Datatype& datatype)
{
  (void) MPI_File_read_at_all_begin(mpi_file, offset, buf, count, datatype);
}


inline void
MPI::File::Read_at_all_end(void* buf)
{
  MPI_Status status;
  (void) MPI_File_read_at_all_end(mpi_file, buf, &status);
}


inline void
MPI::File::Read_at_all_end(void* buf, MPI::Status& status)
{
  (void) MPI_File_read_at_all_end(mpi_file, buf, &status.mpi_status);
}


inline void
MPI::File::Read_ordered(void* buf, int count,
			       const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_read_ordered(mpi_file, buf, count, datatype, &status);
}


inline void
MPI::File::Read_ordered(void* buf, int count,
			       const MPI::Datatype& datatype,
			       MPI::Status& status)
{
  (void) MPI_File_read_ordered(mpi_file, buf, count, datatype,
                               &status.mpi_status);
}


inline void
MPI::File::Read_ordered_begin(void* buf, int count,
				     const MPI::Datatype& datatype)
{
  (void) MPI_File_read_ordered_begin(mpi_file, buf, count, datatype);
}


inline void
MPI::File::Read_ordered_end(void* buf)
{
  MPI_Status status;
  (void) MPI_File_read_ordered_end(mpi_file, buf, &status);
}


inline void
MPI::File::Read_ordered_end(void* buf, MPI::Status& status)
{
  (void) MPI_File_read_ordered_end(mpi_file, buf, &status.mpi_status);
}


inline void
MPI::File::Read_shared(void* buf, int count,
			      const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_read_shared(mpi_file, buf, count, datatype, &status);
}


inline void
MPI::File::Read_shared(void* buf, int count,
			      const MPI::Datatype& datatype,
			      MPI::Status& status)
{
  (void) MPI_File_read_shared(mpi_file, buf, count, datatype,
                              &status.mpi_status);
}

inline void
MPI::File::Seek(MPI::Offset offset, int whence)
{
  (void) MPI_File_seek(mpi_file, offset, whence);
}


inline void
MPI::File::Seek_shared(MPI::Offset offset, int whence)
{
  (void) MPI_File_seek_shared(mpi_file, offset, whence);
}


inline void
MPI::File::Set_atomicity(bool flag)
{
  (void) MPI_File_set_atomicity(mpi_file, flag);
}


inline void
MPI::File::Set_info(const MPI::Info& info)
{
  (void) MPI_File_set_info(mpi_file, info);
}


inline void
MPI::File::Set_size(MPI::Offset size)
{
  (void) MPI_File_set_size(mpi_file, size);
}


inline void
MPI::File::Set_view(MPI::Offset disp,
			   const MPI::Datatype& etype,
			   const MPI::Datatype& filetype,
			   const char* datarep,
			   const MPI::Info& info)
{
  (void) MPI_File_set_view(mpi_file, disp, etype, filetype, const_cast<char *>(datarep),
			   info);
}


inline void
MPI::File::Sync()
{
  (void) MPI_File_sync(mpi_file);
}


inline void
MPI::File::Write(const void* buf, int count,
		      const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_write(mpi_file, const_cast<void *>(buf), count, datatype, &status);
}


inline void
MPI::File::Write(const void* buf, int count,
			const MPI::Datatype& datatype,
			MPI::Status& status)
{
  (void) MPI_File_write(mpi_file, const_cast<void *>(buf), count, datatype,
                        &status.mpi_status);
}


inline void
MPI::File::Write_all(const void* buf, int count,
			    const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_write_all(mpi_file, const_cast<void *>(buf), count, datatype, &status);
}



inline void
MPI::File::Write_all(const void* buf, int count,
			    const MPI::Datatype& datatype,
			    MPI::Status& status)
{
  (void) MPI_File_write_all(mpi_file, const_cast<void *>(buf), count, datatype,
                            &status.mpi_status);
}


inline void
MPI::File::Write_all_begin(const void* buf, int count,
				  const MPI::Datatype& datatype)
{
  (void) MPI_File_write_all_begin(mpi_file, const_cast<void *>(buf), count, datatype);
}


inline void
MPI::File::Write_all_end(const void* buf)
{
  MPI_Status status;
  (void) MPI_File_write_all_end(mpi_file, const_cast<void *>(buf), &status);
}


inline void
MPI::File::Write_all_end(const void* buf, MPI::Status& status)
{
  (void) MPI_File_write_all_end(mpi_file, const_cast<void *>(buf), &status.mpi_status);
}


inline void
MPI::File::Write_at(MPI::Offset offset,
			   const void* buf, int count,
			   const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_write_at(mpi_file, offset, const_cast<void *>(buf), count,
			   datatype, &status);
}


inline void
MPI::File::Write_at(MPI::Offset offset,
			   const void* buf, int count,
			   const MPI::Datatype& datatype,
			   MPI::Status& status)
{
  (void) MPI_File_write_at(mpi_file, offset, const_cast<void *>(buf), count,
			   datatype, &status.mpi_status);
}


inline void
MPI::File::Write_at_all(MPI::Offset offset,
			       const void* buf, int count,
			       const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_write_at_all(mpi_file, offset, const_cast<void *>(buf), count,
			       datatype, &status);
}


inline void
MPI::File::Write_at_all(MPI::Offset offset,
			       const void* buf, int count,
			       const MPI::Datatype& datatype,
			       MPI::Status& status)
{
  (void) MPI_File_write_at_all(mpi_file, offset, const_cast<void *>(buf), count,
			       datatype, &status.mpi_status);
}


inline void
MPI::File::Write_at_all_begin(MPI::Offset offset,
				     const void* buf, int count,
				     const MPI::Datatype& datatype)
{
  (void) MPI_File_write_at_all_begin(mpi_file, offset, const_cast<void *>(buf), count,
				     datatype);
}


inline void
MPI::File::Write_at_all_end(const void* buf)
{
  MPI_Status status;
  (void) MPI_File_write_at_all_end(mpi_file, const_cast<void *>(buf), &status);
}


inline void
MPI::File::Write_at_all_end(const void* buf, MPI::Status& status)
{
  (void) MPI_File_write_at_all_end(mpi_file, const_cast<void *>(buf), &status.mpi_status);
}


inline void
MPI::File::Write_ordered(const void* buf, int count,
			      const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_write_ordered(mpi_file, const_cast<void *>(buf), count, datatype,
				&status);
}


inline void
MPI::File::Write_ordered(const void* buf, int count,
				const MPI::Datatype& datatype,
				MPI::Status& status)
{
  (void) MPI_File_write_ordered(mpi_file, const_cast<void *>(buf), count, datatype,
				&status.mpi_status);
}


inline void
MPI::File::Write_ordered_begin(const void* buf, int count,
				    const MPI::Datatype& datatype)
{
  (void) MPI_File_write_ordered_begin(mpi_file, const_cast<void *>(buf), count, datatype);
}


inline void
MPI::File::Write_ordered_end(const void* buf)
{
  MPI_Status status;
  (void) MPI_File_write_ordered_end(mpi_file, const_cast<void *>(buf), &status);
}


inline void
MPI::File::Write_ordered_end(const void* buf,
				    MPI::Status& status)
{
  (void) MPI_File_write_ordered_end(mpi_file, const_cast<void *>(buf), &status.mpi_status);
}


inline void
MPI::File::Write_shared(const void* buf, int count,
			     const MPI::Datatype& datatype)
{
  MPI_Status status;
  (void) MPI_File_write_shared(mpi_file, const_cast<void *>(buf), count,
			       datatype, &status);
}


inline void
MPI::File::Write_shared(const void* buf, int count,
			     const MPI::Datatype& datatype, MPI::Status& status)
{
  (void) MPI_File_write_shared(mpi_file, const_cast<void *>(buf), count,
			       datatype, &status.mpi_status);
}


inline void
MPI::File::Set_errhandler(const MPI::Errhandler& errhandler) const
{
    (void)MPI_File_set_errhandler(mpi_file, errhandler);
}


inline MPI::Errhandler
MPI::File::Get_errhandler() const
{
    MPI_Errhandler errhandler;
    MPI_File_get_errhandler(mpi_file, &errhandler);
    return errhandler;
}

inline void
MPI::File::Call_errhandler(int errorcode) const
{
  (void) MPI_File_call_errhandler(mpi_file, errorcode);
}
