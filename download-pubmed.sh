#!/bin/bash

for fileno in {1..1114}; do
    wget https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed22n`printf "%04d" $fileno`.xml.gz
done
