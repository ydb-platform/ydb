git clone https://git.libreoffice.org/dictionaries libreoffice-dictionaries
cd libreoffice-dictionaries
git pull
cd ..
find libreoffice-dictionaries -name "hyph_*\.dic" | xargs -I '{}' cp '{}' .
sed -i 's/\r$//' *.dic
rename -- -Latn _Latn *-Latn.dic
