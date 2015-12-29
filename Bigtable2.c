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
#include <omp.h>

#define table_size 1100
#define line_count 172
#define col_count 1
#define timestamp_count 1


typedef struct
{
	int timestamp_value;        
	char cell_value[1024];
} time_stamp;

typedef struct
{
	time_stamp timestamp[timestamp_count];
	char name[1024];
} col_label;

typedef struct
{
	col_label label[col_count];
} col_family;

typedef struct
{
	col_family family1;///column:family=3
	col_family family2;
        col_family family3;
        int value;
        char row_key[1024];
} hashed;

void sort(hashed s[],int sz)
{
    omp_set_nested(1);
    omp_set_num_threads(4);
    int i,j;
    printf("id=%s\n",s[2].row_key);
    //int sz = sizeof(s);
    printf("sz=%d\n",sz);
    int done = 0;
    
    for ( i = 0; i < sz; i++)
      {
         #pragma omp parallel for shared(j)
          for ( j = i+1; j < sz; j++)
                  {
                  if (strcmp(s[i].row_key, s[j].row_key) > 0)
                      {

                      hashed tmp;
                      tmp = s[i];
                      s[i] = s[j];
                      s[j] = tmp;
                     }
                 }
     }
    for ( i = 0; i < sz; i++)
       {
        printf("%d - %s\n", i+1, s[i].row_key);
       }
}


        

int main(int argc, char **argv)
{   
        
	hashed H_table[table_size];
	int rank, numprocs, i,p,nkeys;
	double insert_stime, insert_etime;
	double sort_stime, sort_etime;
	double delete_stime, delete_etime;
	unsigned long id=0;
  	
        FILE *file = fopen("keys.txt","r");
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	int chunk = table_size/numprocs;
        
         
  	/* Inserting  Keys */
        
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
			strcpy(H_table[id].row_key,line);
			H_table[id].value = id;
			//printf("Hash: %s\nID: %d\n", H_table[id].hash, H_table[id].value);
			//printf("rank: %d\n", rank);	
			//sleep(4);
                        
		  }
                  
		  id++;
		}
		if (rank == 0) 
		{    
                        nkeys =(int)id;
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
	int found = 0;double search_stime, search_etime;
	int catch = 0;
	int match = 0;
	if(rank == 0)
	printf("Searching a Key..................\n");
	char search[1024] ="hsa-mir-95\n";
	if (rank == 0) search_stime = MPI_Wtime();
	for (i = chunk*rank; i < chunk*(rank+1); i++)
	{
		if (strcmp(H_table[i].row_key,search) == 0)
		{
                        printf("%d",i);
			found++;
			printf("Match Found!\n");
			printf("Key: %s\n ID: %d\n", H_table[i].row_key,H_table[i].value);
                        //return H_table[i].value;
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
                if ( strcmp(H_table[i].row_key,del) == 0 )
                {
                        found++;
                        printf("Key deleted!\n");
                        strcpy(H_table[i].row_key,"");
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
	
        /* Sorting Lexigraphically*/
         if (rank == 0)
         sort_stime = MPI_Wtime();
         sort(H_table,nkeys);
         if (rank == 0) 
         {
        	sort_etime = MPI_Wtime();
                 printf("Sort time: %10.4f\n\n", sort_etime - sort_stime);       
        
         }
         
	 MPI_Finalize();
       


  	return 0;
}
