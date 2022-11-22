#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

int checking(unsigned int *, long);
int compare(const void *, const void *);
struct arrWithSize createArrWithSize(size_t iniSize);
void deleteArrWithSize(struct arrWithSize* aws);
struct arrSizeTID createArrSizeTID(struct arrWithSize aws, int TID);
void deleteArrSizeTID(struct arrWithSize* aws);
void *psort(void *arg);
void *cutArr(void *arg);
void *joinArr(void *arg);

// global variables
long size;  // size of the array
unsigned int * intarr; // array of random integers

int tNum;
int maxSplitArrSize;
struct arrWithSize *splitArr; //size: tNum
struct arrWithSize *splitsubIntArr;
unsigned int *subIntArr; //size: tNum * tNum
struct arrWithSize **partArr; //size: tNum, partArr[0] size: tNum
struct arrWithSize *finalArr;

struct arrSizeTID *asts;
struct arrSizeTID *pivotsTIDs;

sem_t *bSems;

struct arrWithSize {
  unsigned int *arr;
  size_t size;
};

struct arrSizeTID {
  struct arrWithSize aws;
  int tID; //threadID
};


int main (int argc, char **argv)
{
  //long i, j;
  struct timeval start, end;

  if ((argc != 3)) {
    printf("Usage: seq_sort <number> [<no_of_workers>]\n");
    exit(0);
  }
    
  size = atol(argv[1]);
  tNum = atol(argv[2]);
  maxSplitArrSize = size / tNum + 1;
  pthread_t threads[tNum]; 
  intarr = (unsigned int *)malloc(size*sizeof(unsigned int));
  if (intarr == NULL) {perror("malloc intarr"); exit(0); }

  splitArr = (struct arrWithSize *)malloc(tNum * sizeof(unsigned int));
  if (splitArr == NULL) {perror("malloc splitArr"); exit(0); }

  splitsubIntArr = (struct arrWithSize *)malloc(tNum * sizeof(struct arrWithSize));
  if (splitsubIntArr == NULL) {perror("malloc splitsubIntArr"); exit(0); }

  finalArr = (struct arrWithSize *)malloc(tNum * sizeof(struct arrWithSize));
  if (finalArr == NULL) {perror("malloc splitsubIntArr"); exit(0); }

  asts = (struct arrSizeTID *)malloc(tNum * sizeof(struct arrSizeTID));
  if (asts == NULL) {perror("malloc asts"); exit(0); }

  pivotsTIDs = (struct arrSizeTID *)malloc(tNum * sizeof(struct arrSizeTID));
  if (pivotsTIDs == NULL) {perror("malloc pivotsTIDs"); exit(0); }

  for (int i = 0; i < tNum; i++) {
    splitsubIntArr[i] = createArrWithSize(tNum);
  }

  subIntArr = (unsigned int *)malloc(tNum * tNum * sizeof(unsigned int));
  if (subIntArr == NULL) {perror("malloc subIntArr"); exit(0); }

  partArr = (struct arrWithSize **)malloc(tNum * sizeof(struct arrWithSize *));
  for (int i = 0; i < tNum; i++) {
    partArr[i] = (struct arrWithSize *)malloc(tNum * sizeof(struct arrWithSize));
  }
  

  bSems = (sem_t *)malloc((tNum + 1) * sizeof(sem_t));
  if (bSems == NULL) {perror("malloc bSems"); exit(0); }

  for (int i = 0; i < tNum + 1; i++) {
    sem_init(&bSems[i], 0, 1);
  }
  // set the random seed for generating a fixed random
  // sequence across different runs
  char * env = getenv("RANNUM");  //get the env variable
  if (!env)                       //if not exists
    srandom(3230);
  else
    srandom(atol(env));
  
  for (int i=0; i<size; i++) {
    intarr[i] = random();
  }
  
  //creating split arrays
  for (int i = 0; i < size % tNum; i++) {
    splitArr[i] = createArrWithSize(maxSplitArrSize);
    memcpy(splitArr[i].arr, &intarr[i * maxSplitArrSize], maxSplitArrSize * sizeof(unsigned int));
    splitArr[i].size = maxSplitArrSize;
  }
  for (int i = size % tNum; i < tNum; i++) {
    splitArr[i] = createArrWithSize(maxSplitArrSize - 1);
    memcpy(splitArr[i].arr, &intarr[maxSplitArrSize * (size % tNum) + i * (maxSplitArrSize - 1)], (maxSplitArrSize - 1) * sizeof(unsigned int));
    splitArr[i].size = maxSplitArrSize - 1;
  }
  
  // measure the start time
  gettimeofday(&start, NULL);
  
  // just call the qsort library
  // replace qsort by your parallel sorting algorithm using pthread

  for (int i = 0; i < tNum; i++) {
    int *ptr = malloc(sizeof(int));
    *ptr = i;
    pthread_create(&threads[i], NULL, psort, (void *)ptr);
  }
  
  for (int i = 0; i < tNum; i++) {
		pthread_join(threads[i], NULL);
	}

  for (int i = 0; i < tNum; i++) {
    memcpy(subIntArr + i * tNum, splitsubIntArr[i].arr, tNum * sizeof(unsigned int));
  }

  qsort(subIntArr, tNum * tNum, sizeof(unsigned int), compare);

  unsigned int *pivot = (unsigned int *)malloc((tNum - 1) * sizeof(unsigned int));
  for (int i = 0; i < tNum - 1; i++) {
    pivot[i] = subIntArr[tNum * (i + 1) + (int)(tNum / 2) - 1];
  }

  for (int i = 0; i < tNum; i++) {
    struct arrWithSize arr = createArrWithSize(tNum - 1);
    memcpy(arr.arr, pivot, (tNum - 1) * sizeof(unsigned int));
    arr.size = tNum - 1;
    pivotsTIDs[i] = createArrSizeTID(arr, i);
  }

  for (int i = 0; i < tNum; i++) {
    pthread_create(&threads[i], NULL, cutArr,(void *)&pivotsTIDs[i]);
  }  
  
  for (int i = 0; i < tNum; i++) {
		pthread_join(threads[i], NULL);
	}

  for (int i = 0; i < tNum; i++) {
    finalArr[i] = createArrWithSize(0);
  }
  

  for (int i = 0; i < tNum; i++) {
    int j = i;
    pthread_create(&threads[i], NULL, joinArr,(void *)&j);
  }

  for (int i = 0; i < tNum; i++) {
		pthread_join(threads[i], NULL);
	}

  // measure the end time
  gettimeofday(&end, NULL);
  
  if (!checking(intarr, size)) {
    printf("The array is not in sorted order!!\n");
  }
  
  printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec)*1.0 + (end.tv_usec - start.tv_usec)/1000000.0);
    
  free(intarr);
  return 0;
}

int compare(const void * a, const void * b) {
  return (*(unsigned int *)a>*(unsigned int *)b) ? 1 : ((*(unsigned int *)a==*(unsigned int *)b) ? 0 : -1);
}

int checking(unsigned int * list, long size) {
  long i;
  printf("First : %d\n", list[0]);
  printf("At 25%%: %d\n", list[size/4]);
  printf("At 50%%: %d\n", list[size/2]);
  printf("At 75%%: %d\n", list[3*size/4]);
  printf("Last  : %d\n", list[size-1]);
  for (i=0; i<size-1; i++) {
    if (list[i] > list[i+1]) {
      return 0;
    }
  }
  return 1;
}

struct arrWithSize createArrWithSize(size_t iniSize) {
  struct arrWithSize aws;
  aws.arr = malloc(sizeof(unsigned int) * iniSize);
  aws.size = iniSize;

  return aws;
}

void deleteArrWithSize(struct arrWithSize* aws) {
  free(aws->arr);
}

struct arrSizeTID createArrSizeTID(struct arrWithSize aws, int TID) {
  struct arrSizeTID ast;
  ast.aws = aws;
  ast.tID = TID;

  return ast;
}

void deleteArrSizeTID(struct arrWithSize* aws) {}

void *psort(void *arg) {
  int *tIDaddr = (int *)arg;
  int tID = *tIDaddr;
  sem_wait(&bSems[tID]);
  printf("tID in psort: %d", tID);
  qsort(splitArr[tID].arr, splitArr[tID].size, sizeof(unsigned int), compare);
  if (!checking(splitArr[tID].arr, splitArr[tID].size))
  {
    printf("The split array %d is not in sorted order!!\n", tID);
  }
  sem_post(&bSems[tID]);
  for (int i = 0; i < tNum; i++) {
    splitsubIntArr[tID].arr[i] = splitArr[tID].arr[i * (int)(size / (tNum * tNum))];
  }
  pthread_exit(NULL);
}

void *cutArr(void *arg) {
  struct arrSizeTID *pivotsTID = (struct arrSizeTID *)arg;
  int low = 0;
  int high = splitArr[pivotsTID->tID].size;
  int start = 0;

  for (int i = 0; i < (tNum - 1); i++) {
    while(low <= high) {
      int mid = low + (high - low) / 2;
      if (splitArr[pivotsTID->tID].arr[mid - 1] <= pivotsTID->aws.arr[i]) {
        if (splitArr[pivotsTID->tID].arr[mid] > pivotsTID->aws.arr[i]) {
          partArr[pivotsTID->tID][i] = createArrWithSize(mid - start);
          memcpy(partArr[pivotsTID->tID][i].arr, &splitArr[pivotsTID->tID].arr[start], sizeof(unsigned int) * (mid - start));
          start = mid;
          low = mid;
          high = splitArr[pivotsTID->tID].size;
          break;
        } else {
          low = mid;
        }        
      } else {
        high = mid;
      }
    }
  }
  partArr[pivotsTID->tID][tNum - 1] = createArrWithSize(high - start);
  memcpy(partArr[pivotsTID->tID][tNum - 1].arr, &splitArr[pivotsTID->tID].arr[start], sizeof(unsigned int) * (high - start));

  sem_post(&bSems[pivotsTID->tID]);
  pthread_exit(NULL);
}

void *joinArr(void *arg) {
  int *tIDaddr = (int *)arg;
  int tID = *tIDaddr;
  for (int i = 0; i < tNum; i++) {
    //sem_wait(&bSems[i]);
    int oldSize = finalArr[tID].size;
    int newSize = finalArr[tID].size + partArr[i][tID].size;
    if (newSize == 0) {
      continue;
    }
    
    unsigned int *newArr = realloc(finalArr[tID].arr, newSize * sizeof(unsigned int));
    if (newArr == NULL) {
      free(finalArr[tID].arr);
      exit(0);
    } else {
      finalArr[tID].arr = newArr;
      memcpy(finalArr[tID].arr + oldSize, partArr[i][tID].arr, sizeof(unsigned int) * partArr[i][tID].size);
      finalArr[tID].size = newSize;
    }
    //sem_post(&bSems[i]);
    
  }
  qsort(finalArr[tID].arr, finalArr[tID].size, sizeof(unsigned int), compare);
  


  pthread_exit(NULL);
}
