#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sqlparser.h"

int main()
{
    char input[500];

    printf("--------------------- Database Manager ---------------------\n" );
    for (;;)
    {
        size_t length;

        printf("dbc > ");
        if (fgets(input, sizeof(input), stdin) == NULL)
            return -1;

        length = strlen(input);
        if (input[length - 1] == '\n')
            input[length - 1] = '\0';

        if ((strcmp(input, "exit") == 0) || (strcmp(input, "\\q") == 0))
            return 0;

        SQLExecuteQuery(input);
    }
    return 0;
}
