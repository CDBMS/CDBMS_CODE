#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <io.h>

/* SQL parser lexer state */
enum ScannerState
{
    ScanValue,
    ScanToken
};

/* Enumeration for command types */
enum QueryType
{
    Create,
    Select,
    Delete,
    Insert,
    Update,
    Invalid
};

/* Enumeration for data types */
enum FieldType
{
    Integer,
    Number,
    String,
    Boolean
};

/*
 * Table structure container (maximum 128 columns)
 *
 *   count      : number of columns in the table
 *   columns    : names of table columns
 *   columnTypes: data type of each column
 *   name       : the name of the table
 */
struct TableStructureInfo
{
    size_t count;
    char   columns[128][128];
    enum   FieldType columnTypes[128];
    char   name[128];
};

/* A generic map container for binary search usage */
struct StringIntMap
{
    const char *string;
    enum QueryType type;
};

/* Operators for SQL statments */
enum Operator
{
    InvalidOperator = -1,
    EqualOperator,
    NotEqualOperator,
    GreaterThanOperator,
    LessThanOperator,
    GreaterOrEqualOperator,
    LessOrEqualOperator,
    AssignOperator
};

/* Internal Linked List to generate a primitive an AST */
struct TokenList
{
    char *keyword;
    char *value;
    enum Operator operator;

    struct TokenList *next;
};

/* Truth values */
typedef enum Bool
{
    False,
    True
} Bool;

/* Union to hold the column data, only valid data types are included */
union Value
{
    int integer;
    float number;
    char *string;
    Bool boolean;
};

/*
 * One column of a table:
 *      value   : the column data value
 *      type    : the data type
 *      position: the column position in the table
 */
struct Column
{
    union Value value;
    enum FieldType type;
    int position;
};

/*
 * One row of a table:
 *      index       : the row index
 *      columns     : columns the row columns content
 *      columnsCount: the number of columns in the row
 */
struct Row
{
    int index;
    struct Column columns[128];
    size_t columnCount;
};

/*
 * Table data:
 *      rows    : rows of the table
 *      rowCount: number of rows in the table.
 */
struct Table
{
    struct Row *rows;
    size_t rowCount;
};

/* A map of the SQL commands, allows fast search using binary search */
static const struct StringIntMap QueryTypes[] = {
    {"DATASET", Create},
    {"DELETE", Delete},
    {"INSERT_INTO", Insert},
    {"SELECT", Select},
    {"UPDATE", Update}
};

/* A map of the valid data types, allows fast search using binary search */
static const struct StringIntMap DataTypes[] = {
    {"BOOLEAN", Boolean},
    {"INTEGER", Integer},
    {"NUMBER", Number},
    {"STRING", String}
};

/* String map comparison function, for binary search */
static int compare(const void *const lhs, const void *const rhs)
{
    if ((lhs == NULL) || (rhs == NULL))
        return 0;
    return strcmp(((struct StringIntMap *)lhs)->string, ((struct StringIntMap *)rhs)->string);
}

/* Convert the parsed string to a column value, giving the string and the field type */
union Value SQLvalueFromStringAndType(const char *string, enum FieldType type)
{
    union Value value;

    switch (type)
    {
    case Integer:
        value.integer = strtol(string, NULL, 10);
        break;
    case Boolean:
        value.boolean = strcmp("True", string) ? False : True;
        break;
    case Number:
        value.number = strtod(string, NULL);
        break;
    case String:
        if (string == NULL)
            value.string = NULL;
        else
        {
            size_t length;

            if (*string == '\'')
                string += 1;
            /* create a copy of the string, removing ' characters */
            value.string = strdup(string);
            length       = strlen(value.string);
            if (value.string[length - 1] == '\'')
                value.string[length - 1] = '\0';
        }
        break;
    }

    return value;
}

/* Generic StringIntMap search */
int SQLParser_FindInMap(const char *const query, const struct StringIntMap *const map, int count)
{
    struct StringIntMap  data;
    struct StringIntMap *found;
    size_t               size;

    size        = sizeof(struct StringIntMap);
    data.string = query;
    found       = bsearch(&data, map, count, size, compare);

    if (found == NULL)
        return Invalid;

    return found->type;
}

/* Retrieve Query Type */
enum QueryType SQLParser_GetQueryType(const char *const query)
{
    return SQLParser_FindInMap(query, QueryTypes, sizeof(QueryTypes) / sizeof(QueryTypes[0]));
}

/* Retrieve Field Type */
enum FieldType SQLParser_GetFieldType(const char *const query)
{
    return SQLParser_FindInMap(query, DataTypes, sizeof(DataTypes) / sizeof(DataTypes[0]));
}

/* cleanup Token List */
static void freeTokens(struct TokenList *head)
{
    struct TokenList *current;
    struct TokenList *last;

    current = head;
    while (current != NULL)
    {
        last    = current;
        current = current->next;

        if (last != NULL)
        {
            if (last->value != NULL)
                free(last->value);
            if (last->keyword != NULL)
                free(last->keyword);
            free(last);
        }
    }
}

/*
 * Append element to the TokenList linked list
 *
 *      This function always returns the head, the caller should pass `NULL` as
 *      the first parameter to create a new list, next it always append to then
 *      list and returns the list head node, for example, to use it in a loop
 *
 *      head = NULL;
 *      for (count = 0 ; count < totalCount ; ++count)
 *          head = appendToken(head, keyword, operator, value);
 */
static struct TokenList *appendToken(struct TokenList *head,
        const char *const keyword, enum Operator operator, const char *const value)
{
    struct TokenList *new;

    /* Create a new node */
    new = malloc(sizeof(struct TokenList));
    if (new == NULL)
        return NULL;
    /* Initalize all members */
    new->next     = NULL;
    new->keyword  = strdup(keyword);
    new->value    = strdup(value);
    new->operator = operator;

    /* If head was not yet specified, then the new node is head */
    if (head == NULL)
        head = new;
    else
    {
        /* Seek to the tail of the list, and append the node */
        struct TokenList *current;
        current = head;
        while (current->next != NULL)
            current = current->next;
        current->next = new;
    }
    /* Always return head */
    return head;
}

/* Simple, get operator helper */
enum Operator SQLfindOperator_helper(const char **source)
{
    enum Operator operator;
    const char   *query;

    if ((source == NULL) || (*source == NULL))
        return InvalidOperator;

    query    = *source;
    operator = InvalidOperator;
    if (*query == '>')
    {
        if (*(query + 1) == '=')
        {
            query   += 1;
            operator = GreaterOrEqualOperator;
        }
        else
            operator = GreaterThanOperator;
    }
    else if (*query == '<')
    {
        if (*(query + 1) == '=')
        {
            query   += 1;
            operator = LessOrEqualOperator;
        }
        else if (*(query + 1) == '>')
        {
            query   += 1;
            operator = NotEqualOperator;
        }
        else
            operator = LessThanOperator;
    }
    else if ((*query == '!') && (*query == '='))
    {
        query   += 1;
        operator = NotEqualOperator;
    }
    else if (*query == '=')
        operator = EqualOperator;
    else
        operator = AssignOperator;
    *source = query;

    return operator;
}

struct TokenList *SQLParser_Parse(const char *query)
{
    struct TokenList  *head;
    enum ScannerState  state;
    char              *buffer  = NULL;
    char              *keyword = NULL;
    size_t             index;
    size_t             length;
    enum Operator      operator;

    index  = 0;
    length = strlen(query);
    buffer = malloc(1 + length); /* hold temporary data */
    if (buffer == NULL)
        goto abort;
    keyword = malloc(1 + length); /* hold temporary data */
    if (keyword == NULL)
        goto abort;
    head     = NULL;
    operator = AssignOperator;
    state    = ScanToken;
    while (*query != '\0')
    {
        switch (*query)
        {
        /* If an operator is found, lets save the currently read string into keyword */
        case ':':
        case '<':
        case '>':
        case '!':
        case '=':
            operator = SQLfindOperator_helper(&query);
            /* If we are in ScanToken state, an operator is expected */
            if (state == ScanToken)
            {
                /* remove leading / trailing whitespace */
                while (isspace(buffer[0]) != 0)
                    memmove(buffer, buffer + 1, --index);
                while ((isspace(buffer[index - 1]) != 0) && (index > 0))
                    index--;

                buffer[index] = '\0';
                index         = 0;
                state         = ScanValue;

                /* perform the copy now */
                strcpy(keyword, buffer);

                /* read and skip all spaces after token */
                while ((*(++query) != '\0') && (isspace(*query) != 0));

                continue;
            }
            else /* Unexpected operator while parsing the string */
                goto abort;
            break;
        case '\'':
            /* This means consume all characters until another ' appears */
            if (state != ScanValue)
                goto abort;
            /* consume input until end of text of ' found */
            while ((*(query++) != '\0') && (*query != '\''))
                buffer[index++] = *query;
            buffer[index] = '\0';
            break;
        case ' ':
        case '\t':
        case '\n':
            /* spaces should be ignored, unless they delimit a value */
            if (state == ScanValue) /* if you are serching for a value, this is a delimiter */
            {
                /* Copy the value, and start searching for the next token */
                buffer[index] = '\0';
                index         = 0;
                head          = appendToken(head, keyword, operator, buffer);
                state         = ScanToken;
            }
            break;
        default:
            /* not any of the special characters just consume input */
            buffer[index++] = *query;
            break;
        }
        query++;
    }

    /* Append the last possible value, if no delimiter found -> unlikely */
    if (state == ScanValue)
    {
        buffer[index] = '\0';
        index         = 0;
        head          = appendToken(head, keyword, operator, buffer);
    }
    /* release memory to OS */
    free(keyword);
    free(buffer);

    return head;

    /* This label is to prevent repeating the same code over and over DRY principle */
abort:
    printf("error: cannot parse query.\n");
    if (head != NULL)
        freeTokens(head);
    if (buffer != NULL)
        free(buffer);
    if (keyword != NULL)
        free(keyword);
    return NULL;
}

/* Simple brute force search for a column position */
int SQLParser_FindColumn(const struct TableStructureInfo *const table,
                                               const char *const column)
{
    size_t i;
    if (table == NULL)
        return -1;
    for (i = 0 ; i < table->count ; i++)
    {
        if (strcmp(column, table->columns[i]) == 0)
            return i;
    }
    return -1;
}

/* Send the row to stdout, for printing select results */
void SQLwriteRowToStdout(const struct Row *const row)
{
    size_t i;

    for (i = 0 ; i < row->columnCount ; ++i)
    {
        struct Column column;

        column = row->columns[i];
        switch (column.type) /* Select format specifier and union member depending on type */
        {
        case Integer:
            printf("%10d|\t", column.value.integer);
            break;
        case Boolean:
            printf("%-10s|\t", column.value.boolean ? "True" : "False");
            break;
        case Number:
            printf("%10g|\t", column.value.number);
            break;
        case String:
            printf("%-10s|\t", column.value.string);
            break;
        }
    }
    printf("\n");
}

/* Send row to a FILE * */
void SQLwriteRowToFile(FILE *file, const struct Row *const row)
{
    size_t i;

    fprintf(file, "%d;", row->index);
    for (i = 0 ; i < row->columnCount ; ++i)
    {
        struct Column column;

        column = row->columns[i];
        switch (column.type) /* Select format specifier and union member depending on type */
        {
        case Integer:
            fprintf(file, "%d;", column.value.integer);
            break;
        case Boolean:
            fprintf(file, "%s;", column.value.boolean ? "True" : "False");
            break;
        case Number:
            fprintf(file, "%g;", column.value.number);
            break;
        case String:
            fprintf(file, "'%s';", column.value.string);
            break;
        }
    }
    fprintf(file, "\n");
}

/* Modify the row, and send it to the file */
void SQLupdateRowAndWriteToFile(FILE *file, const struct TableStructureInfo *const tableStructure, struct TokenList *list, struct Row *const row)
{
    if ((file == NULL) || (row == NULL))
        return;
    while (list != NULL)
    {
        int index;
        if ((list->operator == AssignOperator) && ((index = SQLParser_FindColumn(tableStructure, list->keyword)) != -1))
            row->columns[index].value = SQLvalueFromStringAndType(list->value, tableStructure->columnTypes[index]);
        list = list->next;
    }
    SQLwriteRowToFile(file, row);
}

/* Write one row to the file */
void SQLwriteRow(const char *const table, const struct Row *const row)
{
    FILE  *file;

    file = fopen(table, "a+");
    if (file == NULL)
        return;
    SQLwriteRowToFile(file, row);
    fclose(file);
}

/* Print the table to stdout */
void SQLprintTable(struct Table *table)
{
    size_t i;
    for (i = 0 ; i < table->rowCount ; ++i)
        SQLwriteRowToStdout(&(table->rows[i]));
}

/* This function will compare the values, according to their type and corresponding operator */
int SQLcompareValues(const struct TokenList *list, union Value value, enum FieldType type)
{
    if ((list == NULL) || (list->operator == -1))
        return -1;
    switch (type) /* check what type are the operands */
    {
    case Integer:
        {
            int lhs;
            int rhs;

            lhs = strtol(list->value, NULL, 10);
            rhs = value.integer;

            switch (list->operator)
            {
            case EqualOperator:
                return (lhs == rhs);
            case NotEqualOperator:
                return (lhs != rhs);
            case GreaterThanOperator:
                return (lhs > rhs);
            case GreaterOrEqualOperator:
                return (lhs >= rhs);
            case LessThanOperator:
                return (lhs < rhs);
            case LessOrEqualOperator:
                return (lhs >= rhs);
            default:
                return -1;
            }
        }
        break;
    case Number:
        {
            int lhs;
            int rhs;

            lhs = strtod(list->value, NULL);
            rhs = value.number;
            switch (list->operator)
            {
            case EqualOperator:
                return (lhs == rhs);
            case NotEqualOperator:
                return (lhs != rhs);
            case GreaterThanOperator:
                return (lhs > rhs);
            case GreaterOrEqualOperator:
                return (lhs >= rhs);
            case LessThanOperator:
                return (lhs < rhs);
            case LessOrEqualOperator:
                return (lhs >= rhs);
            default:
                return -1;
            }
        }
        break;
    case String:
        {
            switch (list->operator)
            {
            case EqualOperator:
                return (strcmp(list->value, value.string) == 0);
            case NotEqualOperator:
                return (strcmp(list->value, value.string) != 0);
            case GreaterThanOperator:
                return (strcmp(list->value, value.string) > 0);
            case GreaterOrEqualOperator:
                return (strcmp(list->value, value.string) >= 0);
            case LessThanOperator:
                return (strcmp(list->value, value.string) < 0);
            case LessOrEqualOperator:
                return (strcmp(list->value, value.string) <= 0);
            default:
                return -1;
            }
        }
        break;
    case Boolean:
        {
            switch (list->operator)
            {
            case EqualOperator:
            case NotEqualOperator:
            case GreaterThanOperator:
            case GreaterOrEqualOperator:
            case LessThanOperator:
            case LessOrEqualOperator:
                return ((strcmp("True", list->value) == 0) == value.boolean);
            default:
                return -1;
            }
        }
        break;
    default:
        break;
    }
    return 1;
}

/* This function will filter the row according to the conditions in list */
int SQLfilterRow(const struct TokenList *list, const struct TableStructureInfo *const tableStructure, const struct Row *row)
{
    if (list == NULL)
        return 0;
    if ((list = list->next) == NULL)
        return 1;
    while (list != NULL)
    {
        int position;

        /* find column position */
        position = SQLParser_FindColumn(tableStructure, list->keyword);
        if (position != -1) /* if found (-1 == not-found) */ 
        {
            const struct Column *column;
            int                  compared;

            /* get the column, and search the conditions for a match */

            column   = &(row->columns[position]);
            compared = SQLcompareValues(list, column->value, column->type);
            if (compared != -1) /* if the values match (-1 don't-match) */
            {
                if ((position == column->position) && (compared == 0))
                    return 0;
            }
        }
        list = list->next;
    }
    return 1;
}

/* This will read a row from the file */
int
SQLreadRow(FILE *file, const struct TableStructureInfo *const tableStructure, struct Row *row)
{
    if ((tableStructure == NULL) || (row == NULL))
        return 0;
    /* Read one line from the file
     *   128 -> maximum fields with.
     * so the maximum possible length of a line, is 128 * tableStructure->count
     */
    char       line[2 + 128 * tableStructure->count];
    char      *pointer;
    char      *token;
    int        columnIndex;
    struct Row current;

    /* Get the line */
    if (fgets(line, sizeof(line), file) == NULL)
        return 0;
    /* Initialize the row all to 0 */
    memset(&current, 0, sizeof(current));

    pointer     = line;
    columnIndex = -1;
    while ((token = strtok(pointer, ";")) != NULL) /* start tokenizing the string with ';' */
    {
        struct Column column;

        /* next call and all subsequent calls to strtok need pointer == NULL */
        pointer = NULL;
        if (columnIndex > -1) /* if this is the first column, ignor it's just the index */
        {
            enum FieldType type;

            /* Initialize the column internal representation */
            type            = tableStructure->columnTypes[columnIndex];
            column.position = columnIndex;
            column.type     = type;
            column.value    = SQLvalueFromStringAndType(token, type);
            /* Set the columnIndex-th column in the row */
            current.columns[columnIndex] = column;
            /* Increase columnCount in this row */
            current.columnCount++;
        }
        else
            current.index = strtol(token, NULL, 10); /* Get the row index */
        columnIndex++;
    }
    /* Column count was updated 1 too far, fix that */
    current.columnCount -= 1;
    /* Set the row data */
    *row = current;

    return 1;
}

struct Table SQLloadTable(const struct TokenList *list, const struct TableStructureInfo *const tableStructure)
{
    int          index;
    struct Row   row;
    struct Table table;
    FILE        *file;

    if (tableStructure == NULL)
        return table;

    /* Open the table file storage */
    file = fopen(tableStructure->name, "r");
    if (file == NULL)
        return table;

    index      = 0;
    table.rows = NULL;
    /* Start reading rows */
    while (SQLreadRow(file, tableStructure, &row) != 0)
    {
        struct Row *auxiliar;
        if ((list != NULL) && (SQLfilterRow(list, tableStructure, &row) == 0))
            continue;
        /* Increase the table rows array size */
        auxiliar = realloc(table.rows, (1 + index) * sizeof(struct Row));
        if (auxiliar == NULL)
            goto abort;
        /* If there was no problem, initialize the row values */
        table.rows        = auxiliar;
        table.rows[index] = row;
        table.rowCount    = 1 + index;
        index             = table.rowCount;
    }
    /* Close the file */
    fclose(file);
    /* return the filled structure */
    return table;

abort: /* This label is to avoid violating the DRY principle */
    table.rowCount = 0;
    if (table.rows != NULL)
        free(table.rows);
    fclose(file);

    return table;
}

/* Check if this row is valid */
int SQLisValidRow(struct TokenList *list, const struct TableStructureInfo *const tableStructure, struct Row *row)
{
    if (list == NULL)
        return 1;
    if ((row == NULL) || (tableStructure == NULL))
        return 0;
    /* If the row contains more columns than the table, invalid */
    if (row->columnCount > tableStructure->count)
    {
        printf("you specified more columns than avaiable\n");
        return 0;
    }
    /* If there is no column with this name in the table, invalid */
    if (SQLParser_FindColumn(tableStructure, list->keyword) == -1)
    {
        printf("no column `%s` in table `%s`\n", list->keyword, tableStructure->name);
        return 0;
    }
    /* Otherwise, valid */
    return 1;
}

/* The sql select function */
void SQLselect(const struct TokenList *list, const struct TableStructureInfo *const tableStructure)
{
    struct Table table;
    if (tableStructure == NULL)
        return;
    table = SQLloadTable(list, tableStructure);

    SQLprintTable(&table);
}

/* The sql delete function */
void SQLdelete(struct TokenList *list, const struct TableStructureInfo *const tableStructure)
{
    struct Table table;
    size_t       i;
    char         filename[] = "__database_Temporary_XXXXXX"; /* temporary filename */
    FILE        *file;

    if (tableStructure == NULL)
        return;
    if (_mktemp(filename) == NULL)
        return;
    if (filename == NULL)
        return;
    /* Open the temporary file */
    file = fopen(filename, "w");
    if (file == NULL)
        return;
    /* Load all the rows in the table */
    table = SQLloadTable(NULL, tableStructure);
    for (i = 0 ; i < table.rowCount ; i++)
    {
        struct Row *row;
        row = &(table.rows[i]);
        /* If row is invalid, abort the operation */
        if (SQLisValidRow(list->next, tableStructure, row) == 0)
            goto abort;
        /*
         * If the row, does not satisfy the condition, write it back to the storage,
         * otherwise skip it.
         */
        if (SQLfilterRow(list, tableStructure, row) == 0)
            SQLwriteRowToFile(file, row);
    }
    /* close the temporary file */
    fclose(file);

    /* delete the table storage file */
    remove(tableStructure->name);
    /* make the temporary file, the new storage file */
    rename(filename, tableStructure->name);

    return;

abort:
    fclose(file);
    remove(filename);
}

/* The sql delete function */
void SQLupdate(struct TokenList *list, const struct TableStructureInfo *const tableStructure)
{
    struct Table table;
    size_t       i;
    char         filename[] = "__database_Temporary_XXXXXX"; /* temporary filename */
    FILE        *file;

    if (tableStructure == NULL)
        return;
    if (_mktemp(filename) == NULL)
        return;
    if (filename == NULL)
        return;
    /* Open the temporary file */
    file = fopen(filename, "w");
    if (file == NULL)
        return;
    /* Load all the rows in the table */
    table = SQLloadTable(NULL, tableStructure);
    for (i = 0 ; i < table.rowCount ; i++)
    {
        struct Row *row;

        row = &(table.rows[i]);
        /* If row is invalid, abort the operation */
        if (SQLisValidRow(list->next, tableStructure, row) == 0)
            goto abort;
        /* If the row, does not satisfy the condition, write it back to the storage */
        if (SQLfilterRow(list, tableStructure, row) == 0)
            SQLwriteRowToFile(file, row);
        else /* If the row, does satisfy the condition, write the modified row to the storage */
            SQLupdateRowAndWriteToFile(file, tableStructure, list->next, row);
    }
    /* close the temporary file */
    fclose(file);

    /* delete the table storage file */
    remove(tableStructure->name);
    /* make the temporary file, the new storage file */
    rename(filename, tableStructure->name);

    return;

abort:
    fclose(file);
    remove(filename);
}

/* Simple check operators are equal function */
int SQLcheckOperator(enum Operator lhs, enum Operator rhs)
{
    if (lhs != rhs)
        printf("invalid operator for expression.\n");
    return (lhs == rhs);
}

/* The sql insert function */
void SQLinsert(struct TokenList *list, const struct TableStructureInfo *const tableStructure)
{
    struct Row  row;
    const char *tableName;

    if ((list == NULL) || (tableStructure == NULL))
        return;

    tableName       = tableStructure->name;
    row.columnCount = 0;
    list            = list->next;
    row.index       = 0;
    /* Parse the AST to get the row values */
    while (list != NULL)
    {
        struct Column column;

        /* Only assignment operator is valid here */
        if (SQLcheckOperator(list->operator, AssignOperator) == 0)
            return;
        /* Check that this column is valid */
        if (SQLisValidRow(list, tableStructure, &row) == 0)
            return;

        column.type                  = tableStructure->columnTypes[row.columnCount];
        column.value                 = SQLvalueFromStringAndType(list->value, column.type);
        column.position              = SQLParser_FindColumn(tableStructure, list->keyword);
        row.columns[row.columnCount] = column;
        row.columnCount             += 1;
        list                         = list->next;
    }
    /* Append the row to the file */
    SQLwriteRow(tableName, &row);
}

/* This function will create a table in the database */
int SQLParser_CreateTable(struct TokenList *list)
{
    FILE                     *file;
    struct TableStructureInfo info;
    struct TokenList         *current;
    size_t                    length;
    int                       success;

    if (list == NULL)
        return 1;

    /* Open the database internal table structure storage file */
    file = fopen("__tables_data.dat", "a");
    if (file == NULL)
        return 1;
    /* Initialize TableStructureInfo to 0 */
    memset(&info, 0, sizeof(info));

    /* Get the length of the table name, and ensure it can be stored */
    length = strlen(list->keyword);
    if (length > sizeof(info.name) - 1)
        goto abort;
    current = list->next;
    /* Copy the table name */
    memcpy(info.name, list->value, length);
    /* Parse the AST to get all field's names, and types */
    while (current != NULL)
    {
        /* Check that the field name is not too large */
        length = strlen(current->keyword);
        if (length > sizeof(info.columns) - 1)
            goto abort;
        /* Copy field name */
        memcpy(info.columns[info.count], current->keyword, length);

        /* Find the field type enum value, and set that value for current column */
        info.columnTypes[info.count] = SQLParser_GetFieldType(current->value);
        info.count                  += 1;

        /* If there is no more room to store columns, abort */
        if (info.count > sizeof(info.columnTypes))
            goto abort;
        current = current->next;
    }
    /* Write the data to the file */
    success = fwrite(&info, sizeof(info), 1, file);
    /* close the file */
    fclose(file);

    return (success != 1);

abort:
    if (file != NULL)
        fclose(file);
    return 1;
}

/* This function searches for a table in the TableStructureInfo */
struct TableStructureInfo SQLParser_FindTable(const char *const name)
{
    FILE                     *file;
    struct TableStructureInfo table;

    memset(&table, 0, sizeof(table));

    /* Open the storage file for reading */
    file = fopen("__tables_data.dat", "r");
    if (file == NULL)
        return table;
    /* read records until the record is found, or the end of file is reached */
    while (fread(&table, sizeof(struct TableStructureInfo), 1, file) == 1)
    {
        if (strcmp(name, table.name) == 0)
        {
            fclose(file);
            return table;
        }
    }
    /* Reset the table structure to return an invalid value */
    memset(&table, 0, sizeof(table));

    return table;
}

/* Execute query function */
int SQLExecuteQuery(const char *const query)
{
    struct TokenList         *list;
    struct TableStructureInfo table;

    list = SQLParser_Parse(query);
    if (list == NULL)
        return 1;

    /* Find the queried table */
    table = SQLParser_FindTable(list->value);
    switch (SQLParser_GetQueryType(list->keyword)) /* Check the command and call the right function */
    {
        case Create:
            if (table.name[0] == '\0')
                SQLParser_CreateTable(list);
            else
                printf("there is a table with the same name, cannot create table `%s`\n", list->value);
            break;
        case Select:
            SQLselect(list, &table);
            break;
        case Update:
            SQLupdate(list, &table);
            break;
        case Insert:
            SQLinsert(list, &table);
            break;
        case Delete:
            SQLdelete(list, &table);
            break;
        default:
            break;
    }
    freeTokens(list);

    return 0;
}
