/*****************************************
*	explain: Send logistic Data, server side
*	author: zhangyuming
*	date 2014-7-20
*
*********************************************************/
#include	<stdlib.h>
#include	<stdio.h>
#include	<string.h>
#include 	<sys/types.h>
#include	<sys/socket.h>
#include	<netinet/in.h>
#include	<sys/time.h>
#include	<time.h>
#include	<signal.h>
#include	<unistd.h>
#include	<errno.h>


#define	SERVER_TCP_PORT	9999
#define	BUFFERSIZE			1400
#define	SENDLINES			10

int main( int argc, char **argv ){
	char 	*fileName = NULL;
	FILE 	*fp = NULL;
	char		*readBuffer = NULL;
	int		serverSocketFd = 0, clientSocketFd = 0, port = 0;
	struct	sockaddr_in serverSocket, clientSocket;
	socklen_t	clientSocketLength = 0;
	int		sendBytes = 0, sendLinesPerSecond = 0;
	int		sleepTime = 0;
	int 		count = 0;
	
	
	switch( argc ){
	case 2:
		port = SERVER_TCP_PORT; 
		sendLinesPerSecond = SENDLINES;
		fileName = argv[1];
		break;
	case 3:
		fileName = argv[1];
		port = atoi(argv[2]); 
		sendLinesPerSecond = SENDLINES;
		break;
	case 4:
		fileName = argv[1];
		port = atoi(argv[2]); 
		sendLinesPerSecond = atoi(argv[3]); 
		break;
	default:
		fprintf( stderr, "Usage: %s date_file_name [port] [sendLines/s]\n", argv[0] );
		return -1;
	}
	sleepTime = 1000000/sendLinesPerSecond;
	printf("[ATTENTION] we read max 1400 char in a line\n");
	if (NULL == (fp = fopen(fileName, "r" )))
	{
		fprintf(stderr, "%s%d\t Can't open %s file.\n", __FILE__, __LINE__, fileName);
		return -1;
	}
	if ( NULL == (readBuffer = (char *)malloc(BUFFERSIZE*sizeof(char))))
	{
		printf("%s%d\t malloc error\n", __FILE__, __LINE__);
		return -1;
	}
	if ( NULL == fgets(readBuffer, BUFFERSIZE, fp) )
	{
		fprintf(stderr, "%s%d\t fgets error.\n", __FILE__, __LINE__);
		return -1;
	}
	//printf("[TEST] %s\n", readBuffer);

	//Create a stream socket
	if (-1 == (serverSocketFd = socket(AF_INET, SOCK_STREAM, 0)))
	{
		fprintf(stderr, "%s%d\t  Can't create a socket\n",  __FILE__, __LINE__);
		return -1;
	}
	//Bind an address to the socket
	bzero((char *)&serverSocket, sizeof(struct sockaddr_in));
	serverSocket.sin_family = AF_INET;
	serverSocket.sin_port = htons( port );
	serverSocket.sin_addr.s_addr = htonl( INADDR_ANY );
	if (-1 == bind(serverSocketFd, (struct sockaddr *)&serverSocket, sizeof(serverSocket)))
	{
		fprintf( stderr, "%s%d\t  Can't bind name to  socket\n",  __FILE__, __LINE__ );
		fprintf( stderr, "%s%d\t  [ERROR] %s\n",  __FILE__, __LINE__, strerror(errno));
		return -1;
	}
	listen(serverSocketFd, 1);
	printf( "The server is prepared OK !\n" );

	//handle connection
	clientSocketLength = sizeof(clientSocket);
	if (-1 == (clientSocketFd = accept(serverSocketFd,
			(struct sockaddr *)&clientSocket, &clientSocketLength)))
	{
		fprintf(stderr, "%s%d\t  Can't accept client\n",  __FILE__, __LINE__);
		return -1;
	}

	count = 0;
	do
	{
		//send data to the client
		if (-1 == (sendBytes= send(clientSocketFd, readBuffer, BUFFERSIZE, 0)))
		{
			fprintf(stderr, "%s%d\t Sendto error\n", __FILE__, __LINE__);
			return -1;
		}
		usleep(sleepTime);
		count ++;
		if (0 == (count+1)%5000)
		{
			printf("We have send 5000 lines in the past!\n");
		}
	}while(NULL != fgets(readBuffer, BUFFERSIZE, fp));

	printf("The data has all been sended! Wait 10 seconds!\n");
	sleep(100);
	
	return 0;
}









