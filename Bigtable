#include <stdio.h>
#include <string.h>
#include "mpi.h"
#include <dirent.h>
#include <stdlib.h>
#include "mpi.h"
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>
#include <time.h>

#define table_size 1723
#define line_count 1723

typedef struct
{
	char hash[1024];
	int value;
} hashed;


int main(int argc, char **argv)
{   
	hashed H_table[table_size];
	int rank, numprocs, i;
	double insert_stime, insert_etime;
	double search_stime, search_etime;
	double delete_stime, delete_etime;
	unsigned long id=0;
  	FILE *file = fopen("mirna.txt","r");
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	int chunk = table_size/numprocs;
        
         
  	/* Insert Keys */
        
	if (rank == 0)
	printf("Adding Keys..................\n");
	if (file != NULL) 
	{
		char line[1024];
               
		if (rank == 0) insert_stime = MPI_Wtime();
		while (fgets(line,sizeof(line),file) != NULL)
		{
	           
		  if ( id >= chunk*rank && id < chunk*(rank+1) )
	          {	
			strcpy(H_table[id].hash,line);
			H_table[id].value = id;
			//printf("Hash: %s\nID: %d\n", H_table[id].hash, H_table[id].value);
			//printf("rank: %d\n", rank);	
			//sleep(4);
                        
		  }
                  
		  id++;
		}
		if (rank == 0) 
		{
			insert_etime = MPI_Wtime();
			printf("Number of Keys added: %lu \n", id);
			printf("Insert time: %10.4f\n\n", insert_etime - insert_stime);
		}
		fclose(file);
	}
	else 
	{
		printf("I/O error!\n");
	}
       
	/* Search by Key */ 
	int found = 0;
	int catch = 0;
	int match = 0;
	if(rank == 0)
	printf("Searching a Key..................\n");
	char search[1024] ="hsa-mir-95\n";
	if (rank == 0) search_stime = MPI_Wtime();
	for (i = chunk*rank; i < chunk*(rank+1); i++)
	{
		if (strcmp(H_table[i].hash,search) == 0)
		{
                        printf("%d",i);
			found++;
			printf("Match Found!\n");
			printf("Key: %s\n ID: %d\n", H_table[i].hash, H_table[i].value);
			break;
		}
	}	
	if ( rank != 0)
	{
		MPI_Send(&found, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
	}
	if ( rank == 0 )
	{
		for (i=1;i<numprocs;i++)
		{
			MPI_Recv(&catch, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			if (catch == 1) match = 1;
		}
		if (match == 0 && found == 0) printf("No Match Found!\n");
	}
        if (rank == 0) 
        {
        	search_etime = MPI_Wtime();
                printf("Search time: %10.4f\n\n", search_etime - search_stime);
        }
         
	/* Delete Key */
        found = 0;
        catch = 0;
        match = 0;
	if(rank == 0)
	printf("Deleting a Key..................\n");
        char del[1024] = "miRNA_ID\n";
	if (rank == 0) delete_stime = MPI_Wtime();
        for (i = chunk*rank; i < chunk*(rank+1); i++)
        {
                if ( strcmp(H_table[i].hash,del) == 0 )
                {
                        found++;
                        printf("Key deleted!\n");
                        strcpy(H_table[i].hash,"");
			H_table[i].value = -1;
                        break;
                }
        }
        if ( rank != 0)
        {
                MPI_Send(&found, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        }
        if ( rank == 0 )
        {
                for (i=1;i<numprocs;i++)
                {
                        MPI_Recv(&catch, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        if (catch == 1) match = 1;
                }
                if (match == 0 && found == 0) printf("Cannot Delete Key, Key not Found!\n");
        }
        if (rank == 0) 
        {
        	delete_etime = MPI_Wtime();
                printf("Delete time: %10.4f\n\n", delete_etime - delete_stime);
        }
	
	MPI_Finalize();
       
  	return 0;
}
