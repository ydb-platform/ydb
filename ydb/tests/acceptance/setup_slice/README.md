This is an instruction how to create slice in Yandex Cloud for tests.

1. Get or generate ssh key for the slices access. 
Put it in some location, e.g. `~/.ssh/id_ed25519.pub`

2. Create VMs in Yandex Cloud 
Sample code:
```sh
export FOLDER=b1grf3mpoatgflnlavjd
export VM_PREFIX=slice-0
for i in 1 2 3 4 5 6 7 8 ; do yc compute instance create $VM_PREFIX$i --zone ru-central1-d --cores 4 --memory 8 --hostname $VM_prefix$i --create-boot-disk size=200,image-folder-id=standard-images,image-family=ubuntu-2204-lts  --ssh-key ~/.ssh/id_ed25519.pub --folder-id $FOLDER ; done
```

3. Create systemd units and file drives on VMs
```sh
cd tests/acceptance/setup_slice
ya make -r .
./setup_slice --cluster ../cluster.yaml 
```

4. Use ydbd_slice
