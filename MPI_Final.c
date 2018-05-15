/* 	COMPILE: mpicc MPI_Final.c -lm -o <Executable> 
	RUN: mpirun -np 4 ./<Executable> #This indicates 4 processes */

#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>
#include <mpi.h>
#include <sys/time.h>

#define FileValueBuffer 10
#define NumberofFiles 1 //Number of input files -1
//struct that is able to hold a key and a string of 255 characters
typedef struct entry_type
{
	int Key;
	char Value[FileValueBuffer];
} entry;

void merge (entry C[], int size, entry temp[]);
void mergesort_serial (entry C[], int size, entry temp[]);
void mergesort_parallel_mpi (entry C[], int size, entry temp[], int level, int my_rank, int max_rank, int tag, MPI_Comm comm, MPI_Datatype mpi_entry);

int file_size(char filename[])
{
	int lines = 0;
	char ch;
	FILE *fp;
	fp = fopen(filename, "r");
	while (!feof(fp))
	{
		ch = fgetc(fp);
		if (ch == '\n')
		{
			lines++;
		}
	}
	fclose(fp);
	return lines;
}

bool read_files(entry C[], int size, entry temp[], int numFiles, char* fileNames[])
{
	FILE *fp;
	char* token;
	char* val;
	int comma = ',';
	char buff[255];//size of buffer
	int counter = 0;
	for (int i = 0; i < numFiles; i++)
	{
		fp = fopen(fileNames[i],"r");		
		while (fgets(buff, 255, fp))
		{
			token = strtok(buff, ",");
			C[counter].Key = atoi(token);
			token = strtok(NULL, "\n");
			strcpy(C[counter].Value,token);
			counter++;
		}
		fclose(fp);
	}
	if (counter == size)
	{
		return true;
	}
	else
	{
		return false;
	}
}

void write_file(entry C[], int size, MPI_Datatype mpi_entry, int rank, int comm_size)
{
	/*
	how many comparisons == size 
	size/numprocessors == amount each processor handles
	size%numprocessors == amount root process handles
	*/
	FILE *fp;
	fp = fopen("OutputTableC.csv", "a+");
	int rootPrint = size%comm_size;
	int processPrint = size/comm_size;
	//append sections of the array to the output depending on the rank
	if (rank == 0)
	{
		for (int i = 0; i < rootPrint+processPrint; i++)
		{
			if (C[i].Key == C[i+NumberofFiles].Key)
			{
				fprintf(fp, "%d,", C[i].Key);
				fprintf(fp, "%s,%s\n", C[i].Value, C[i+NumberofFiles].Value);
			}
		}
	}
	else
	{
		int startPrint = rank*processPrint+rootPrint;
		for (int i = startPrint; i < startPrint+processPrint; i++)
		{
			if (C[i].Key == C[i+NumberofFiles].Key)
			{
				fprintf(fp, "%d,", C[i].Key);
				fprintf(fp, "%s,%s\n", C[i].Value, C[i+NumberofFiles].Value);
			}
		}
	}
	fclose(fp);
	return;
}

int main (int argc, char *argv[])
{
	//all processes
	MPI_Init (&argc, &argv);
	//check process rank
	//communicator size is the number of processes
	FILE *fp;
	fp = fopen("OutputTableC.csv", "w+");
	fclose(fp);
	int comm_size;
	MPI_Comm_size (MPI_COMM_WORLD, &comm_size);
	int my_rank;
	MPI_Comm_rank (MPI_COMM_WORLD, &my_rank);
	int max_rank = comm_size - 1;
	int tag = 123;
	bool outputFlag = false;

	//create struct to use as a data type in MPI
	int blocklengths[2] = {1, FileValueBuffer};
	MPI_Datatype types[2] = {MPI_INT, MPI_BYTE};
	MPI_Datatype mpi_entry;
	MPI_Aint offsets[2];
	offsets[0] = offsetof(struct entry_type, Key);
	offsets[1] = offsetof(struct entry_type, Value);
	MPI_Type_create_struct(2, blocklengths, offsets, types, &mpi_entry);
	MPI_Type_commit(&mpi_entry);

	// Set test data
	if (my_rank == 0)
	{
		//root process 
		int size = 0;	// Array size
		int numFiles = NumberofFiles+1;

		char* fileNames[numFiles];
		for (int i = 0; i < numFiles; i++)
		{
			fileNames[i] = malloc(sizeof(char)*255);
		}
		//determine number of lines
		fileNames[0] = "TableA.csv";
		fileNames[1] = "TableB.csv";

		for (int i = 0; i < numFiles; i++)
		{
			size += file_size(fileNames[i]);
		}

		printf ("Array size = %d\nProcesses = %d\n", size, comm_size);
		//read files
		entry C[size];
		entry temp[size];
		if (read_files(C, size, temp, numFiles, fileNames))
		{
			//initiate sort with root process
			double beginTimer, endTimer;
			beginTimer = MPI_Wtime();
				mergesort_parallel_mpi (C, size, temp, 0, my_rank, max_rank, tag, MPI_COMM_WORLD, mpi_entry);
				MPI_Request request;
				for (int i = 1; i < comm_size; i++)
				{
					MPI_Request request;
					//asynchronous send of sorted array
					MPI_Isend(C, size, mpi_entry, i, tag, MPI_COMM_WORLD, &request);
				}
				//write to output in parallel
				write_file(C, size, mpi_entry, my_rank, comm_size);
			endTimer = MPI_Wtime();
			printf ("Begin= %.8f\nEnd = %.8f\nElapsed = %.8f\n", beginTimer, endTimer, endTimer - beginTimer);

		}
		else
		{
			MPI_Finalize ();
			printf("Error");
			return 0;
		}
	}
	else
	{
		//helper processes
		int level = 0;
		while (pow (2, level) <= my_rank) level++;
		MPI_Status status;
		int size;
		//probe for a message from a source
		MPI_Probe (MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
		//determine message size and sender
		MPI_Get_count (&status, mpi_entry, &size);
		int parent_rank = status.MPI_SOURCE;
		while (!outputFlag)	
		{
			entry C[size];
			entry temp[size];
			MPI_Recv (C, size, mpi_entry, parent_rank, tag, MPI_COMM_WORLD, &status);
			mergesort_parallel_mpi (C, size, temp, level, my_rank, max_rank, tag, MPI_COMM_WORLD, mpi_entry);
			//send sorted array to parent process after mergesort
			MPI_Send (C, size, mpi_entry, parent_rank, tag, MPI_COMM_WORLD);
			outputFlag = true;
		}
		//probe for a message from a source
		MPI_Probe (MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
		//determine message size and sender
		MPI_Get_count (&status, mpi_entry, &size);
		entry C[size];
		MPI_Recv (C, size, mpi_entry, status.MPI_SOURCE, tag, MPI_COMM_WORLD, &status);
		//write to output in parallel
		write_file(C, size, mpi_entry, my_rank, comm_size);
	}
	MPI_Finalize ();
	return 0;
}

void mergesort_parallel_mpi (entry C[], int size, entry temp[], int level, int my_rank, int max_rank, int tag, MPI_Comm comm, MPI_Datatype mpi_entry)
{
    int helper_rank = my_rank + pow (2, level);
    if (helper_rank > max_rank)
    {				
		// no more processes available, therefore sort in serial
        mergesort_serial (C, size, temp);
    }
    else
    {
        MPI_Request request;
        MPI_Status status;
        //asynchronous send of second half
        MPI_Isend (C + size / 2, size - size / 2, mpi_entry, helper_rank, tag, comm, &request);
        //sort first half
        mergesort_parallel_mpi (C, size / 2, temp, level + 1, my_rank, max_rank, tag, comm, mpi_entry);
        //asynchronous request free, transfer complete
        MPI_Request_free (&request);
        //receive sorted second half
        MPI_Recv (C + size / 2, size - size / 2, mpi_entry, helper_rank, tag, comm, &status);
        //merge the sorted arrays
        merge (C, size, temp);
    }
    return;
}

void mergesort_serial(entry C[], int size, entry temp[])
{
	if (size > 1)
	{
		mergesort_serial(C, size/2, temp);
		mergesort_serial(C+size/2, size-size/2, temp);
		merge(C, size, temp);
	}
}

void merge(entry C[], int size, entry temp[])
{
	int m = size/2;
	int n1 = m;//m-l+1;
	int n2 = size;//r-m;
	
	int i = 0;
	int j = m;
	int k = 0;
	//copy C into a temporary array
	for (int l = 0; l < size; l++)
	{
		temp[l] = C[l];
	}
	//swap values if they are smaller than or equal to the compared value
	while ( (i < n1) && (j < n2) )
	{
		if (temp[i].Key <= temp[j].Key)
		{
			C[k] = temp[i];
			i++;
		}
		else
		{
			C[k] = temp[j];
			j++;
		}
		k++;
	}
	//swap any remaining elements if any remain
	while (i < n1)
	{
		C[k] = temp[i];
		i++;
		k++;
	}
	while (j < n2)
	{
		C[k] = temp[j];
		j++;
		k++;
	}
}
