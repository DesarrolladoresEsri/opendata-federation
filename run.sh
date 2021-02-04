# an example checking if the pandas package is installed
if /mnt/c/Users/rmartin/.conda/envs/federation/python.exe -c 'import pkgutil; exit(not pkgutil.find_loader("sodapy"))'; then
    echo 'sodapy found'
    /mnt/c/Users/rmartin/.conda/envs/federation/python.exe main.py
else
    echo 'sodapy not found'
fi