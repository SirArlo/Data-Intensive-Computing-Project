## Completed By:
Arlo Eardley 1108472
Carel Ross 1106684
Ryan Verpoort 1136745

## Details of Repository

The code is available in the master branch and the report is located in 
the Documentation branch of the repoository located here:
https://github.com/SirArlo/Data-Intensive-Computing-Project

### Contents of master branch
* The executable for the multithreaded algorithm is the file "OpenMP" and the executable for the multi-processing algorithm is "MPI".

* Tables A and B each have 100 values in them, for test demonstartion purposes. Output table C has 100 values which is the join of Table A and Table B.

* File OutputTableC_MPI.csv is the result from the multi-processing join of Table A and Table B.
* File OutputTableC_OpenMP.csv is the result from the multi-threaded join of Table A and Table B.

## Using the OpenMP algorithm

* To compile and run the multi-threaded algorithm: 
	* COMPILE: gcc -fopenmp OpenMP_Final.c -o OpenMP
	* RUN: ./OpenMP

* To change the number of threads running on the multi-threaded algorithm change the "NumberOfThreads" variable in the main function of "OpenMP_Final.c"

## Using the MPI algorithm

* To compile and run the multi-processed algorithm (for four processes):
	* COMPILE: mpicc MPI_Final.c -lm -o MPI
	* RUN: mpirun -np 4 ./MPI 

* To change the number of processes running on the multi-processed algorithm change the <#processes> argument in the the terminal command: mpirun -np <#processes> ./MPI




