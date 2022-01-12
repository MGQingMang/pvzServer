/*****************************************

线程池测试

结构体Job：函数指针，参数
任务队列job_head；函数：job_create();job_get();

线程池：20个线程
每个线程包括：线程函数thread_do；存放任务的临界区thread_buf及信号量mutex;一个FLAG，标志此线程是否在工作中；

线程函数thread_do:
1.wait()
2.被唤醒后，从缓冲区中取任务信息，执行；
3.执行结束，清空缓冲区

任务分配函数job_alloc:
1.不断检测任务队列，若有任务，取出；
2.若无空闲线程且线程数未达上限，创建线程；
3.将任务放入线程池的缓冲区thread_buf，并用signal()唤醒线程

*************************************************/

#include <iostream>
#include <process.h>
#include <cstdlib>
#include <winsock2.h>
#include <mutex>
#include <Windows.h>
#pragma comment(lib,"ws2_32.lib")
using namespace std;

const int THREADSIZE = 20;

typedef struct _job 
{										/*Job:任务，包含函数及其变量*/
	int ( *function)(void*);
	void* arg;

	struct _job* next;
}Job;

typedef struct _thread 
{										/*Thread:线程，包含一个buf用来存储任务，一个flag，flag为1表示正在被使用*/
	Job buf;
	int flag;
}Thread;

typedef struct _thpool 
{
	Job* jobHead = NULL;				/*任务队列*/

	Thread thread[THREADSIZE];			/*线程池中的线程*/
	int threadTot = 0;
	int threadUsing = 0;

	HANDLE mutexJob;
	HANDLE mutexThread;
	
}Thpool;

Thpool thpool;

void jobCreate(int ( *function)(void*), void* arg) 
{
	WaitForSingleObject(thpool.mutexJob, INFINITE);
	Job* curr = thpool.jobHead;
	for (; curr->next != NULL; curr = curr->next);
	curr->next = (Job*)malloc(sizeof(Job));
	curr = curr->next;
	curr->next = NULL;
	curr->function = function;
	curr->arg = arg;
	ReleaseSemaphore(thpool.mutexJob, 1, NULL);

	cout << "job_create\tcreate a job" << endl;
}

Job* jobGet(Job* job) 
{
	WaitForSingleObject(thpool.mutexJob, INFINITE);
	Job* jobp = thpool.jobHead->next;
	thpool.jobHead->next = thpool.jobHead->next->next;
	ReleaseSemaphore(thpool.mutexJob, 1, NULL);
	*job = *(jobp);
	free(jobp);

	cout << "job_get\t\tget a job" << endl;
	return job;
}

unsigned __stdcall threadDo(void* param) 
{
	int thread_id = *(int*)param;
	WaitForSingleObject(thpool.mutexThread, INFINITE);
	thpool.threadTot++;
	ReleaseSemaphore(thpool.mutexThread, 1, NULL);

	cout << "thread_do\tthread " << thread_id << " start" << endl;
	
	while (1) 
	{
		WaitForSingleObject(thpool.mutexThread, INFINITE);
		if (1 == ::thpool.thread[thread_id].flag) 
		{
						
			Job job_do = ::thpool.thread[thread_id].buf;
			ReleaseSemaphore(thpool.mutexThread, 1, NULL);
			cout << "thread_do\tready to do a job" << endl;
			job_do.function(job_do.arg);
			WaitForSingleObject(thpool.mutexThread, INFINITE);
			::thpool.thread[thread_id].flag = 0;
			thpool.threadUsing--;
			cout << "thread_do\tthread " << thread_id << " free" << endl;
			
			memset(&::thpool.thread[thread_id], 0, sizeof(Job));
			
		}
		ReleaseSemaphore(thpool.mutexThread, 1, NULL);
		Sleep(100);
	}
	return 0;
}

unsigned __stdcall jobAlloc(void* param) 
{
	while (1) 
	{
		WaitForSingleObject(thpool.mutexJob, INFINITE);
		if (NULL != thpool.jobHead->next) 
		{
			ReleaseSemaphore(thpool.mutexJob, 1, NULL);

			WaitForSingleObject(thpool.mutexThread, INFINITE);
			if (thpool.threadUsing == thpool.threadTot && thpool.threadTot < THREADSIZE) 
			{
				_beginthreadex(NULL, 0, threadDo, &thpool.threadTot, 0, 0);
				cout << "job_alloc\tcreate a thread, tot " << thpool.threadTot << endl;
			}

			int i = 0;
			for (; i < THREADSIZE && ::thpool.thread[i].flag != 0; i++);//找到空闲线程
			if (i < THREADSIZE) 
			{
				jobGet(&::thpool.thread[i].buf);
				cout << "job_alloc\tput a job into thread, thread id: " << i << endl;
				thpool.threadUsing++;
				//::thread[i].Mutex.unlock();
				::thpool.thread[i].flag = 1;
				cout << "job_alloc\tthread " << i << " occupied" << endl;
			}
			ReleaseSemaphore(thpool.mutexThread, 1, NULL);
		}
		else
		{
			ReleaseSemaphore(thpool.mutexJob, 1, NULL);
		}
		Sleep(100);
	}
}

int printNum(int* num)
{
	int temp = *num;
	Sleep(5000);
	cout << "out:" << temp << endl;
	return 0;
}

int thpoolMain()
{
	thpool.jobHead = (Job*)malloc(sizeof(Job));
	thpool.jobHead->next = NULL;
	thpool.mutexJob = CreateSemaphore(NULL, 1, 1, "mutex_job");
	thpool.mutexThread = CreateSemaphore(NULL, 1, 1, "mutex_thread");

	for (int i = 0; i < THREADSIZE; i++) 
	{
		//thread[i].mutex = CreateSemaphore(NULL, 0, 1, "mutex_thread");
		//thpool.thread[i].Mutex.lock();
		thpool.thread[i].flag = 0;
	}

	_beginthreadex(NULL, 0, jobAlloc, NULL, 0, 0);
	cout << "begin thread job_alloc" << endl;
		
	return 0;
}



