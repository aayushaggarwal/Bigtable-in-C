# Bigtable-in-C
Bigtable is a High Performance Data Storage System for storing petabytes of data  It is sparse,distributed,persistent multidimensional in nature   It is Implemented in C using openMP and MPI

Dependencies: openMP and MPI on cluster 

To run:
 mpicc -fopenmp Bigtable2.c -o Bigtlel mrun -np 6 ./Bigtable
