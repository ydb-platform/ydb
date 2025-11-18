sed -e "s|std::replace.*base.*begin.*|std::replace(base.MutRef().begin(), base.MutRef().end(), '-', '_');|" -i src/compiler/python_generator.cc
