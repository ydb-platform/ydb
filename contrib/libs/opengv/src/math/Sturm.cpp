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


#include <opengv/math/Sturm.hpp>
#include <iostream>



opengv::math::Bracket::Bracket( double lowerBound, double upperBound ) :
    _lowerBound(lowerBound),
    _upperBound(upperBound),
    _lowerBoundChangesComputed(false),
    _upperBoundChangesComputed(false),
    _lowerBoundChanges(0),
    _upperBoundChanges(0)
{}

opengv::math::Bracket::Bracket(
    double lowerBound, double upperBound, size_t changes, bool setUpperBoundChanges ) :
    _lowerBound(lowerBound),
    _upperBound(upperBound),
    _lowerBoundChangesComputed(false),
    _upperBoundChangesComputed(false),
    _lowerBoundChanges(0),
    _upperBoundChanges(0)
{
  if( setUpperBoundChanges )
  {
    _upperBoundChanges = changes;
    _upperBoundChangesComputed = true;
  }
  else
  {
    _lowerBoundChanges = changes;
    _lowerBoundChangesComputed = true;
  }
}

opengv::math::Bracket::~Bracket()
{}
  
bool
opengv::math::Bracket::dividable( double eps ) const
{
  if( numberRoots() == 1 && (_upperBound - _lowerBound ) < eps )
    return false;
  if( numberRoots() == 0 )
    return false;
  double center = (_upperBound + _lowerBound) / 2.0;
  if( center == _upperBound || center == _lowerBound)
    return false;
  return true;
}

void
opengv::math::Bracket::divide( std::list<Ptr> & brackets ) const
{
  double center = (_upperBound + _lowerBound) / 2.0;
  Ptr lowerBracket(new Bracket(_lowerBound,center,_lowerBoundChanges,false));
  Ptr upperBracket(new Bracket(center,_upperBound,_upperBoundChanges,true));
  brackets.push_back(lowerBracket);
  brackets.push_back(upperBracket);
}

double
opengv::math::Bracket::lowerBound() const
{
  return _lowerBound;
}

double
opengv::math::Bracket::upperBound() const
{
  return _upperBound;
}

bool
opengv::math::Bracket::lowerBoundChangesComputed() const
{
  return _lowerBoundChangesComputed;
}

bool
opengv::math::Bracket::upperBoundChangesComputed() const
{
  return _upperBoundChangesComputed;
}

void
opengv::math::Bracket::setLowerBoundChanges( size_t changes )
{
  _lowerBoundChanges = changes;
  _lowerBoundChangesComputed = true;
}

void
opengv::math::Bracket::setUpperBoundChanges( size_t changes )
{
  _upperBoundChanges = changes;
  _upperBoundChangesComputed = true;
}

size_t
opengv::math::Bracket::numberRoots() const
{
  if( !_lowerBoundChangesComputed || !_upperBoundChangesComputed )
  {
    std::cout << "Error: cannot evaluate number of roots" <<std::endl;
    return 0;
  }
  return _lowerBoundChanges - _upperBoundChanges;
}


opengv::math::Sturm::Sturm( const Eigen::MatrixXd & p ) :
    _C(Eigen::MatrixXd(p.cols(),p.cols()))
{
  _dimension = (size_t) _C.cols();
  _C = Eigen::MatrixXd(_dimension,_dimension);
  _C.setZero();
  _C.row(0) = p;

  for( size_t i = 1; i < _dimension; i++ )
    _C(1,i) = _C(0,i-1) * (_dimension-i);

  for( size_t i = 2; i < _dimension; i++ )
  {
    Eigen::MatrixXd p1 = _C.block(i-2,i-2,1,_dimension-(i-2));
    Eigen::MatrixXd p2 = _C.block(i-1,i-1,1,_dimension-(i-1));
    Eigen::MatrixXd r;
    computeNegatedRemainder(p1,p2,r);
    _C.block(i,i,1,_dimension-i) = r.block(0,2,1,_dimension-i);
  }
}

opengv::math::Sturm::Sturm( const std::vector<double> & p ) :
    _C(Eigen::MatrixXd(p.size(),p.size()))
{
  _dimension = (size_t) _C.cols();
  _C = Eigen::MatrixXd(_dimension,_dimension);
  _C.setZero();

  for( size_t i = 0; i < _dimension; i++ )
    _C(0,i) = p[i];

  for( size_t i = 1; i < _dimension; i++ )
    _C(1,i) = _C(0,i-1) * (_dimension-i);

  for( size_t i = 2; i < _dimension; i++ )
  {
    Eigen::MatrixXd p1 = _C.block(i-2,i-2,1,_dimension-(i-2));
    Eigen::MatrixXd p2 = _C.block(i-1,i-1,1,_dimension-(i-1));
    Eigen::MatrixXd r;
    computeNegatedRemainder(p1,p2,r);
    _C.block(i,i,1,_dimension-i) = r.block(0,2,1,_dimension-i);
  }
}

opengv::math::Sturm::~Sturm() {};

void
opengv::math::Sturm::findRoots2( std::vector<double> & roots, double eps_x, double eps_val )
{
  double absoluteBound = computeLagrangianBound();
  std::list<Bracket::Ptr> stack;
  stack.push_back(Bracket::Ptr(new Bracket(-absoluteBound,absoluteBound)));
  stack.back()->setLowerBoundChanges( evaluateChain2(stack.back()->lowerBound()) );
  stack.back()->setUpperBoundChanges( evaluateChain2(stack.back()->upperBound()) );
  roots.reserve(stack.back()->numberRoots());
  
  //some variables for pollishing
  Eigen::MatrixXd monomials(_dimension,1);
  monomials(_dimension-1,0) = 1.0;
  
  while( !stack.empty() )
  {  
    Bracket::Ptr bracket = stack.front();
    stack.pop_front();
    
    if( bracket->dividable(eps_x) )
    {
      bool divide = true;
      
      if( bracket->numberRoots() == 1 )
      {
        //new part, we try immediately to do the pollishing here
        bool converged = false;
        
        double root = 0.5 * (bracket->lowerBound() + bracket->upperBound());
        for(size_t i = 2; i <= _dimension; i++)
          monomials(_dimension-i,0) = monomials(_dimension-i+1,0)*root;
        Eigen::MatrixXd matValue = _C.row(0) * monomials;
        
        double value = matValue(0,0);
        
        while( !converged )
        {
          Eigen::MatrixXd matDerivative = _C.row(1) * monomials;
          double derivative = matDerivative(0,0);
          
          double newRoot = root - (value/derivative);
          
          if( newRoot < bracket->lowerBound() || newRoot > bracket->upperBound() )
            break;
          
          for(size_t i = 2; i <= _dimension; i++)
            monomials(_dimension-i,0) = monomials(_dimension-i+1,0)*newRoot;
          matValue = _C.row(0) * monomials;
          
          double newValue = matValue(0,0);
          
          if( fabs(newValue) > fabs(value) )
            break;
          
          //do update
          value = newValue;
          root = newRoot;
          
          //check if converged
          if( fabs(value) < eps_val )
            converged = true;
        }
        
        if( converged )
        {
          divide = false;
          roots.push_back(root);
        }
      }
      
      if(divide)
      {
        bracket->divide(stack);
        std::list<Bracket::Ptr>::iterator it = stack.end();
        it--;
        size_t changes = evaluateChain2((*it)->lowerBound());
        (*it)->setLowerBoundChanges(changes);
        it--;
        (*it)->setUpperBoundChanges(changes);
      }
    }
    else
    {
      if( bracket->numberRoots() > 0 )
        roots.push_back(0.5 * (bracket->lowerBound() + bracket->upperBound()));
    }
  }
}

std::vector<double>
opengv::math::Sturm::findRoots()
{
  //bracket the roots
  std::vector<double> roots;
  bracketRoots(roots);

  //pollish
  Eigen::MatrixXd monomials(_dimension,1);
  monomials(_dimension-1,0) = 1.0;

  for(size_t r = 0; r < roots.size(); r++ )
  {
    //Now do Gauss iterations here
    //evaluate all monomials at the left bound
    for(size_t k = 0; k < 5; k++ )
    {
      for(size_t i = 2; i <= _dimension; i++)
        monomials(_dimension-i,0) = monomials(_dimension-i+1,0)*roots[r];

      Eigen::MatrixXd value = _C.row(0) * monomials;
      Eigen::MatrixXd derivative = _C.row(1) * monomials;

      //correction
      roots[r] = roots[r] - (value(0,0)/derivative(0,0));
    }
  }

  return roots;
}

void
opengv::math::Sturm::bracketRoots( std::vector<double> & roots, double eps )
{
  double absoluteBound = computeLagrangianBound();
  std::list<Bracket::Ptr> stack;
  stack.push_back(Bracket::Ptr(new Bracket(-absoluteBound,absoluteBound)));
  stack.back()->setLowerBoundChanges( evaluateChain2(stack.back()->lowerBound()) );
  stack.back()->setUpperBoundChanges( evaluateChain2(stack.back()->upperBound()) );
  
  double localEps = eps;
  if( eps < 0.0 )
    localEps = absoluteBound / (10.0 * stack.back()->numberRoots());
  roots.reserve(stack.back()->numberRoots());
  
  while( !stack.empty() )
  {  
    Bracket::Ptr bracket = stack.front();
    stack.pop_front();
    
    if( bracket->dividable( localEps) )
    {
      bracket->divide(stack);
      std::list<Bracket::Ptr>::iterator it = stack.end();
      it--;
      size_t changes = evaluateChain2((*it)->lowerBound());
      (*it)->setLowerBoundChanges(changes);
      it--;
      (*it)->setUpperBoundChanges(changes);
    }
    else
    {
      if( bracket->numberRoots() > 0 )
        roots.push_back(0.5 * (bracket->lowerBound() + bracket->upperBound()));
    }
  }
}

size_t
opengv::math::Sturm::evaluateChain( double bound )
{
  Eigen::MatrixXd monomials(_dimension,1);
  monomials(_dimension-1,0) = 1.0;
  
  //evaluate all monomials at the bound
  for(size_t i = 2; i <= _dimension; i++)
    monomials(_dimension-i,0) = monomials(_dimension-i+1,0)*bound;
  
  Eigen::MatrixXd signs(_dimension,1);
  for( size_t i = 0; i < _dimension; i++ )
    signs.block(i,0,1,1) = _C.block(i,i,1,_dimension-i) * monomials.block(i,0,_dimension-i,1);
  
  bool positive = false;
  if( signs(0,0) > 0.0 )
    positive = true;
  
  int signChanges = 0;
  
  for( size_t i = 1; i < _dimension; i++ )
  {
    if( positive )
    {
      if( signs(i,0) < 0.0 )
      {
        signChanges++;
        positive = false;
      }
    }
    else
    {
      if( signs(i,0) > 0.0 )
      {
        signChanges++;
        positive = true;
      }
    }
  }
  
  return signChanges;
}

size_t
opengv::math::Sturm::evaluateChain2( double bound )
{  
  std::vector<double> monomials;
  monomials.resize(_dimension);
  monomials[_dimension-1] = 1.0;
  
  //evaluate all monomials at the bound
  for(size_t i = 2; i <= _dimension; i++)
    monomials[_dimension-i] = monomials[_dimension-i+1]*bound;
  
  std::vector<double> signs;
  signs.reserve(_dimension);
  for( size_t i = 0; i < _dimension; i++ )
  {
    signs.push_back(0.0);
    for( size_t j = i; j < _dimension; j++ )
      signs.back() += _C(i,j) * monomials[j];
  }
  
  bool positive = false;
  if( signs[0] > 0.0 )
    positive = true;
  
  int signChanges = 0;
  
  for( size_t i = 1; i < _dimension; i++ )
  {
    if( positive )
    {
      if( signs[i] < 0.0 )
      {
        signChanges++;
        positive = false;
      }
    }
    else
    {
      if( signs[i] > 0.0 )
      {
        signChanges++;
        positive = true;
      }
    }
  }
  
  return signChanges;
}

void
opengv::math::Sturm::computeNegatedRemainder(
    const Eigen::MatrixXd & p1,
    const Eigen::MatrixXd & p2,
    Eigen::MatrixXd & r )
{
  //we have to create 3 subtraction polynomials
  Eigen::MatrixXd poly_1(1,p1.cols());
  poly_1.block(0,0,1,p2.cols()) = (p1(0,0)/p2(0,0)) * p2;
  poly_1(0,p1.cols()-1) = 0.0;

  Eigen::MatrixXd poly_2(1,p1.cols());
  poly_2.block(0,1,1,p2.cols()) = (p1(0,1)/p2(0,0)) * p2;
  poly_2(0,0) = 0.0;

  Eigen::MatrixXd poly_3(1,p1.cols());
  poly_3.block(0,1,1,p2.cols()) = (-p2(0,1)*p1(0,0)/pow(p2(0,0),2)) * p2;
  poly_3(0,0) = 0.0;

  //compute remainder
  r = -p1 + poly_1 + poly_2 + poly_3;
}

double
opengv::math::Sturm::computeLagrangianBound()
{
  std::vector<double> coefficients;
  coefficients.reserve(_dimension-1);

  for(size_t i=0; i < _dimension-1; i++)
    coefficients.push_back(pow(fabs(_C(0,i+1)/_C(0,0)),(1.0/(i+1))));

  size_t j = 0;
  double max1 = -1.0;
  for( size_t i = 0; i < coefficients.size(); i++ )
  {
    if( coefficients[i] > max1 )
    {
      j = i;
      max1 = coefficients[i];
    }
  }

  coefficients[j] = -1.0;

  double max2 = -1.0;
  for( size_t i=0; i < coefficients.size(); i++ )
  {
    if( coefficients[i] > max2 )
      max2 = coefficients[i];
  }

  double bound = max1 + max2;
  return bound;
}
