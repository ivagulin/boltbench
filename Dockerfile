FROM redhat/ubi9-minimal

COPY ./boltbench /boltbench

ENTRYPOINT ["/boltbench", "--rwmode=false"]