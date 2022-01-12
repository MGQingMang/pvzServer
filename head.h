#pragma once

#include <iostream>
#include <WinSock2.h>
using namespace std;

const int PORT = 8000;
const int BUFSIZE = 1024;
const int STRSIZE = 128;
const int PARAMSIZE = 10;

typedef struct _client 
{
	SOCKET cSocket;		/*客户端套接字*/
	unsigned int id;		/*客户端序号*/
	char IP[20];			/*客户端IP*/
	unsigned short port;

	char buf[BUFSIZE];

	DWORD threadId;

	char username[STRSIZE];

	int flag;

	struct _client* next;
}Client;

typedef struct _request 
{
	char type[STRSIZE];
	char param[PARAMSIZE][STRSIZE];
}Request;

typedef struct _match 
{
	Client* client[2];
	DWORD id[2];
}Match;

int thpoolMain();
void jobCreate(int( *function)(void*), void* arg);