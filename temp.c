#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(int argc, char *argv[])
{
	char *cur_path = "home/ranley/Desktop/data.txt";
    char *str = strdup(cur_path);
    char *temp;
    while ((temp = strsep(&str, "/")) != NULL)
	{
        if (strcmp(temp, "") == 0)
		{
			printf("root\n");
		}
        printf("temp : %s \n", temp);
        printf("str : %s \n", str);

    }
}