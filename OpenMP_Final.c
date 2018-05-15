/* 	COMPILE: gcc -fopenmp OpenMP_Final.c -o <Executable>
	RUN: ./<Executable> */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <stdbool.h>
#include "omp.h"

#define FileValueBuffer 10
#define NumberofFiles 1 //Number of input files -1
//struct that is able to hold a key and a string of 255 characters
typedef struct entry
{
	int Key;
	char Value[FileValueBuffer];
} entry;

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

void mergesort_serial(entry C[], int size, entry temp[])
{
	if (size > 1)
	{
		mergesort_serial(C, size/2, temp);
		mergesort_serial(C+size/2, size-size/2, temp);
		merge(C, size, temp);
	}
}

void mergesort_parallel_omp(entry C[], int size, entry temp[])
{
	//in this case running arrays less than 1000 is faster in serial
	if (size < 1000)
	{		
		mergesort_serial(C, size/2, temp);
		mergesort_serial(C+size/2, size-size/2, temp);
		merge(C, size, temp);
	}
	else
	{
		#pragma omp task //if (size > 1000)		
		mergesort_parallel_omp(C, size/2, temp);
		//#pragma omp task if (size > 1000)
		mergesort_parallel_omp(C+size/2, size-size/2, temp+size/2);
		
		#pragma omp taskwait
		merge(C, size, temp);
	}
}

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

void write_file(entry C[], int size, int SizeOfChunk)
{
	FILE *fp;
	fp = fopen("OutputTableC.csv", "w+");
	#pragma omp parallel
	{
		#pragma omp for schedule(dynamic, SizeOfChunk) nowait
		for (int i = 0; i < size-NumberofFiles; i++)
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

int main(int argc, char *argv[]) 
{
	int NumberOfThreads = 4;
	int SizeOfChunk = 4;
	//omp_set_nested(1);
	omp_set_num_threads(NumberOfThreads);
	
	int size = 0;

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
	//read files
	entry C[size];
	entry temp[size];

	if (read_files(C, size, temp, numFiles, fileNames))
	{
		/*double StartSerial = omp_get_wtime();
			mergesort_serial(C, size, temp);
		double StopSerial = omp_get_wtime() - StartSerial;*/
		double StartOpenMP = omp_get_wtime();
			#pragma omp parallel
				#pragma omp single
				mergesort_parallel_omp(C, size, temp);
			//write to output in parallel	
			write_file(C, size, SizeOfChunk);
		double StopOpenMP = omp_get_wtime() - StartOpenMP;
			//printf("\nSerial Time  : %lf\n", StopSerial);
			printf("\nParallel Thread Time: %.8f\n", StopOpenMP);
			//printf("Size %d\n", size);
	}
	else
	{
		printf("Error");
	}
	return 0;
}
