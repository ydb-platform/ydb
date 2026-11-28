ya make -r . 
echo "### "
echo "### use-after-free"
echo "### "
./gwpasan use-after-free
echo "### "
echo "### bounds"
echo "### "
./gwpasan bounds