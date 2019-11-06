#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#define FILE "mnt/afile.txt"

int main(int argc, char** argv){
	int res=0;
	int fd2=open(FILE, O_RDWR|O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	if (fd2!=-1){
		int wr=write(fd2,"hello\n",6);
		if(wr==-1){
			res=errno;
			perror("write");
		}
		struct stat statbuf;
		int sr=stat(FILE, &statbuf);
		if(sr!=0){
			res=errno;
			perror("stat");
		}
	}else{
		res=errno;
		perror("open");
	}
	return res;
}
