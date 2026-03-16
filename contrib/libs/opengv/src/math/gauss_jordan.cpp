/******************************************************************************
 * Author:   Laurent Kneip                                                    *
 * Contact:  kneip.laurent@gmail.com                                          *
 * License:  Copyright (c) 2013 Laurent Kneip, ANU. All rights reserved.      *
 *                                                                            *
 * Redistribution and use in source and binary forms, with or without         *
 * modification, are permitted provided that the following conditions         *
 * are met:                                                                   *
 * * Redistributions of source code must retain the above copyright           *
 *   notice, this list of conditions and the following disclaimer.            *
 * * Redistributions in binary form must reproduce the above copyright        *
 *   notice, this list of conditions and the following disclaimer in the      *
 *   documentation and/or other materials provided with the distribution.     *
 * * Neither the name of ANU nor the names of its contributors may be         *
 *   used to endorse or promote products derived from this software without   *
 *   specific prior written permission.                                       *
 *                                                                            *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"*
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  *
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE *
 * ARE DISCLAIMED. IN NO EVENT SHALL ANU OR THE CONTRIBUTORS BE LIABLE        *
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL *
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR *
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER *
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT         *
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY  *
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF     *
 * SUCH DAMAGE.                                                               *
 ******************************************************************************/


#include <opengv/math/gauss_jordan.hpp>
#include <math.h>

void
opengv::math::gauss_jordan(
    std::vector<std::vector<double>*> & matrix, int exitCondition )
{  
  //create some constants for zero-checking etc.
  double precision = 1.0e-10;
  int rows = matrix.size();
  int cols = matrix[0]->size();
  
  //the working row
  int frontRow;
  
  //first step down
  for( frontRow = 0; frontRow < rows; frontRow++ )
  {
    // first iterate through the rows and find the row that has the biggest
    // leading coefficient
    double maxValue = -1.0;
    int row = -1;
    for( int tempRow = frontRow; tempRow < rows; tempRow++ )
    {
      double value = fabs((*(matrix[tempRow]))[frontRow]);
      if( value > maxValue )
      {
        row = tempRow;
        maxValue = value;
      }
    }
    
    //rowIter is now the row that should go in the place of frontRow->swap    
    std::swap( matrix[row], matrix[frontRow] );
    
    //ok, now use frontRow!
    
    //first divide all coefficients by the leading coefficient
    int col = frontRow;
    double leadingCoefficient_inv = 1.0 / (*(matrix[frontRow]))[col];
    (*(matrix[frontRow]))[col] = 1.0;
    ++col;
    while( col < cols )
    {
      (*(matrix[frontRow]))[col] *= leadingCoefficient_inv;
      ++col;
    }
    
    //create a vector of bools indicating the cols that need to be manipulated
    col = frontRow;
    std::vector<int> nonzeroIdx;
    nonzeroIdx.reserve( cols - frontRow );
    while( col < cols )
    {
      if( fabs(((*(matrix[frontRow]))[col])) > precision )
        nonzeroIdx.push_back(col);
      col++;
    }
    
    //iterate through all remaining rows, and subtract correct multiple of 
    //first row (if leading coefficient is non-zero!)
    row = frontRow;
    ++row;
    while( row < rows )
    {
      col = frontRow;
      double leadingCoefficient = (*(matrix[row]))[col];
      
      if( fabs(leadingCoefficient) > precision )
      {
        for( int col = 0; col < (int) nonzeroIdx.size(); col++ )
          (*(matrix[row]))[nonzeroIdx[col]] -=
              leadingCoefficient * ((*(matrix[frontRow]))[nonzeroIdx[col]]);
      }
      
      ++row;
    }
  }
  
  //set index to the last non-zero row
  --frontRow;
  
  //Now step up
  while( frontRow > exitCondition )
  {    
    //create a vector of bools indicating the cols that need to be manipulated
    int col = frontRow;
    std::vector<int> nonzeroIdx;
    nonzeroIdx.reserve( cols - frontRow );
    while( col < cols )
    {
      if( fabs(((*(matrix[frontRow]))[col])) > precision )
        nonzeroIdx.push_back(col);
      col++;
    }
    
    //get the working row
    int row = frontRow;
    
    do
    {      
      //decrement working row
      --row;
      
      //working column
      
      //now get the leading coefficient
      double leadingCoefficient = (*(matrix[row]))[frontRow];
      
      //Now iterator until the end, and subtract each time the multiplied
      //front-row
      if( fabs(leadingCoefficient) > precision )
      {        
        for( int col = 0; col < (int) nonzeroIdx.size(); col++ )
          (*(matrix[row]))[nonzeroIdx[col]] -=
              leadingCoefficient * (*(matrix[frontRow]))[nonzeroIdx[col]];
      }
    }
    while( row > exitCondition );
    
    --frontRow;
  }
}
