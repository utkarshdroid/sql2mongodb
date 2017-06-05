using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoMigration
{
    public class Migrator
    {
        public Dictionary<string, string> splitters = new Dictionary<string, string>() {
            { "!=", "$ne"},
            { "<>", "$ne"},
            { ">=", "$gte"},
            { "<=", "$lte"},
            { "!>", "$lte"},
            { "!<", "$gte"},
            { "in", "$in"},
            { "not in", "$nin"},
            { ">", "$gt"},
            { "<", "$lt"},
            { "=", "$eq" },
            { "not like", "$regex" },
            { "like", "$regex" }
        };
        public Dictionary<string, string> JSsplitters = new Dictionary<string, string>() {
            { "!=", "!="},
            { "<>", "!="},
            { ">=", ">="},
            { "<=", "<="},
            { "!>", "<="},
            { "!<", ">="},
            //{ "in", "$in"},
            //{ "not in", "$nin"},
            { ">", ">"},
            { "<", "<"},
            { "=", "==" }
            //{ "not like", "$regex" },
            //{ "like", "$regex" }
        };
        public Dictionary<string, string> tokens = new Dictionary<string, string>()
        {
            { "declare", "var" },
            { ":=", "=" },
            { "begin", "{" },
            { "end", "}" }
        };
        public Dictionary<string, string> fills = new Dictionary<string, string>();
        public string storedJS(string procName, List<Param> paramList, string value)
        {
            return "db.system.js.save({_id: \"" + procName + "\", value: " + value + "} }";
        }

        public string getEquiMongo(string sql)
        {
            string o = "";
            foreach (string op in splitters.Keys.ToArray())
            {
                if (sql.Contains(op))
                {
                    if (sql[sql.IndexOf(op) - 1] != '$')
                    {
                        o = op;
                        break;
                    }
                }
            }
            if (o != "")
            {
                string[] operation = sql.Split(new string[] { o }, StringSplitOptions.None).Select(a => a.Trim()).ToArray();

                string column = operation[0];
                string operand = operation[1].EndsWith(";") ? operation[1].Remove(operation[1].Length - 1, 1) : operation[1];

                if (o == "not like" || o == "like")
                {
                    operand = operand.Substring(1, operand.Length - 2);
                    string newOperand = "";
                    for (int i = 0; i < operand.Length; i++)
                    {
                        if (operand[i] == '%')
                            newOperand += ".*";
                        else if (operand[i] == '_')
                            newOperand += '.';
                        else
                            newOperand += operand[i];
                    }
                    operand = "/^((?!" + newOperand + ").)*$/";
                    //operand = '/' + operand + '/';
                }

                return column + ": {" + splitters[o] + ": " + operand + "}";
            }
            return sql;
        }

        public string getEquiJS(string sql)
        {
            string o = "";
            foreach (string op in JSsplitters.Keys.ToArray())
            {
                if (sql.Contains(op))
                {
                    if (sql[sql.IndexOf(op) - 1] != '$')
                    {
                        o = op;
                        break;
                    }
                }
            }
            if (o != "")
            {
                string[] operation = sql.Split(new string[] { o }, StringSplitOptions.None).Select(a => a.Trim()).ToArray();

                string column = operation[0].StartsWith("@") ? operation[0].Substring(1) : operation[0];
                string operand = operation[1].EndsWith(";") ? operation[1].Remove(operation[1].Length - 1, 1) : operation[1];

                if (o == "not like" || o == "like")
                {
                    operand = operand.Substring(1, operand.Length - 2);
                    string newOperand = "";
                    for (int i = 0; i < operand.Length; i++)
                    {
                        if (operand[i] == '%')
                            newOperand += ".*";
                        else if (operand[i] == '_')
                            newOperand += '.';
                        else
                            newOperand += operand[i];
                    }
                    operand = "/^((?!" + newOperand + ").)*$/";
                    //operand = '/' + operand + '/';
                }

                return column + " " + JSsplitters[o] + " " + operand;
            }
            return sql;
        }

        public string conditional(string input)
        {
            string temp = "";

            Dictionary<string, string> newFills = bracketResolver(input);

            if (newFills.Count > 0)
            {
                temp = newFills["temp"];
                newFills.Remove("temp");

                string a = "";
                string[] temps = { };
                string separator = "";
                if (temp.Contains("and"))
                {
                    temps = temp.Split(new String[] { "and" }, StringSplitOptions.None).Select(z => z.Trim()).ToArray();
                    separator = " && ";
                    //a = "$and : [";
                }
                else if (temp.Contains("or"))
                {
                    temps = temp.Split(new String[] { "or" }, StringSplitOptions.None).Select(z => z.Trim()).ToArray();
                    separator = " || ";
                    //a = "$or : [";
                }

                for (int i = 0; i < temps.Length; i++)
                {
                    string q = temps[i];
                    string abc = conditional(Array.IndexOf(newFills.Keys.ToArray(), q) == -1 ? q : newFills[q]);
                    a += (i == temps.Length - 1 ? abc : abc + separator);
                }
                //a += "]";
                return a;
            }
            Console.WriteLine();

            int start = 0, end = input.Length;
            if (input.IndexOf("(") > -1)
            {
                start = input.LastIndexOf("(") + 1;
                end = input.IndexOf(")");
            }

            string operation = input.Substring(start, end - start);

            string[] parts = whereParts(operation);
            List<string> equiJSParts = new List<string>();
            foreach (string part in parts)
            {
                equiJSParts.Add(getEquiJS(part));
            }

            string s = "";
            if (operation.Contains("and"))
            {
                //x = "$and : [" + x + "]";
                s = " && ";

            }
            else if (operation.Contains("or"))
            {
                //x = "$or : [" + x + "]";
                s = " || ";
            }

            string x = "( ";
            for (int i = 0; i < equiJSParts.Count; i++)
            {
                x += equiJSParts[i] + (i < equiJSParts.Count - 1 ? s : ") ");
            }

            

            if (input.IndexOf("(") == -1)
                return x;

            int index = fills.Count;
            string key = "{{{-" + index + "-}}}";

            fills.Add(key, x);

            string prettyString = input.Substring(0, start - 1) + key + input.Substring(end + 1);
            return conditional(prettyString);
        }

        public Dictionary<string, string> bracketResolver(string input)
        {
            string temp = "";
            Dictionary<string, string> newFills = new Dictionary<string, string>();
            List<string> expressions = new List<string>();
            if (input.Contains("("))
            {
                int startIndex = -1, endIndex = -1;
                int total = 0, count = 0;
                for (int i = 0; i < input.Length; i++)
                {
                    if (input[i] == '(')
                    {
                        if (total == 0)
                        {
                            temp += input.Substring(endIndex + 1, i - endIndex - 1);

                            endIndex = -1;
                        }
                        total += 1;
                        if (startIndex == -1) startIndex = i + 1;
                    }
                    else if (input[i] == ')')
                    {
                        total -= 1;
                        if (total == 0 && endIndex == -1) endIndex = i;
                    }

                    if (startIndex > -1 && endIndex > -1 && total == 0)
                    {
                        count += 1;
                        string sub = input.Substring(startIndex, endIndex - startIndex);
                        newFills.Add("{{{--" + count + "--}}}", sub);
                        temp += "{{{--" + count + "--}}}";
                        startIndex = -1;
                    }
                }
                newFills.Add("temp", temp);
            }
            return newFills;
        }

        public string bracketPrettifier(string input)
        {
            string temp = "";

            Dictionary<string, string> newFills = bracketResolver(input);

            if (newFills.Count > 0)
            {
                temp = newFills["temp"];
                newFills.Remove("temp");

                string a = "";
                string[] temps = { };
                if (temp.Contains("and"))
                {
                    temps = temp.Split(new String[] { "and" }, StringSplitOptions.None).Select(z => z.Trim()).ToArray();
                    a = "$and : [";
                }
                else if (temp.Contains("or"))
                {
                    temps = temp.Split(new String[] { "or" }, StringSplitOptions.None).Select(z => z.Trim()).ToArray();
                    a = "$or : [";
                }
                
                for (int i = 0; i < temps.Length; i++)
                {
                    string q = temps[i];
                    string abc = bracketPrettifier(Array.IndexOf(newFills.Keys.ToArray(), q) == -1 ? q : newFills[q]);
                    a += i == temps.Length - 1 ? abc : abc + ", ";
                }
                a += "]";
                return a;
            }
            Console.WriteLine();

            int start = 0, end = input.Length;
            if (input.IndexOf("(") > -1)
            {
                start = input.LastIndexOf("(") + 1;
                end = input.IndexOf(")");
            }

            string operation = input.Substring(start, end - start);

            string[] parts = whereParts(operation);
            List<string> equiJSParts = new List<string>();
            foreach (string part in parts)
            {
                equiJSParts.Add(getEquiMongo(part));
            }

            string x = "";
            for (int i = 0; i < equiJSParts.Count; i++)
            {
                x += equiJSParts[i] + (i < equiJSParts.Count - 1 ? ", " : " ");
            }

            if (operation.Contains("and"))
            {
                x = "$and : [" + x + "]";
            } else if (operation.Contains("or"))
            {
                x = "$or : [" + x + "]";
            }

            if (input.IndexOf("(") == -1)
                return x;
            int index = fills.Count;
            string key = "{{{-" + index + "-}}}";

            fills.Add(key, x);

            string prettyString = input.Substring(0, start - 1) + key + input.Substring(end + 1);
            return bracketPrettifier(prettyString);
        }

        public string[] whereParts(string whereClause)
        {
            return whereClause.ToLower().Split(new string[] { "and", "or" }, StringSplitOptions.None).Select(a => a.Trim()).ToArray();
        }

        public string setQuery (string setClause)
        {
            return "{" + setClause.Replace('=', ':') + "}";
        }

        public string whereQuery(string whereClause)
        {
            //string[] parts = whereParts(whereClause);
            //List<string> equiJSParts = new List<string>();
            //foreach(string part in parts)
            //{
            //    equiJSParts.Add(getEquiMongo(part));
            //}

            //string x = "";
            //for (int i = 0; i < equiJSParts.Count; i++)
            //{
            //    x += equiJSParts[i] + (i < equiJSParts.Count - 1 ? ", " : " ");
            //}
            //// TO DO
            //return x;
            string q = bracketPrettifier(whereClause);
            foreach(string key in fills.Keys.ToArray())
            {
                q = q.Replace(key, fills[key]);
            }
            fills = new Dictionary<string, string>();
            return q;
        }

        public string combiner(List<string> inputs, List<Param> paramList)
        {

            string paramString = "";
            for (int i = 0; i < paramList.Count; i++)
            {
                paramString += paramList[i].Name + (i == paramList.Count - 1 ? "" : ", ");
            }

            string x = "";
            foreach(string input in inputs)
            {
                x += input + "\r\n";
            }
            string output = @"function(" + paramString + ") { \r\n" + x + "\r\n}";
            return output;
        }
        public string delete(string deleteStatement)
        {
            string[] blocks = deleteStatement.Replace('\n', ' ').Split(' ').Select(a => a.Trim()).Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
            string collection = blocks[2];

            string whereClause = deleteStatement.Split(new string[] { "where" }, StringSplitOptions.None)[1].Trim();
            string js = @"db." + collection + ".remove(" + whereQuery(whereClause) + ");";

            return js;
        }
        public string update(string updateStatement)
        {
            string[] blocks = updateStatement.Replace('\n', ' ').Split(' ').Select(a => a.Trim()).Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
            string collection = blocks[1];

            string[] setWhereClause = updateStatement.Split(new string[] { "set", "where" }, StringSplitOptions.None).Select(a => a.Trim()).ToArray();
            string setClause = setWhereClause[1];
            string whereClause = setWhereClause[2];
            Console.WriteLine(setClause + "\n" + whereClause);
            string js = @"db." + collection + ".update( {" + whereQuery(whereClause) + "}, " + setQuery(setClause) + ", { multi: true });";
            return js;
        }
        public string select(string selectStatement)
        {
            string[] blocks = selectStatement.Split(new string[] { "select", "from" }, StringSplitOptions.None).Select(a => a.Trim()).Where(a => a != "").ToArray();
            string selectedColumnsString = blocks[0];

            string select = "";
            if(selectedColumnsString != "*")
            {
                string[] selectedColums = selectedColumnsString.Split(',').Select(a => a.Trim()).ToArray();
                select = "{";
                for(int i = 0; i < selectedColums.Length; i++)
                {
                    select += selectedColums[i] + " : 1" + (i == selectedColums.Length - 1 ? "" : ", ");
                }
                select += "}";
            }

            string[] where = blocks[1].Split(new string[] { "where" }, StringSplitOptions.None).Select(a => a.Trim()).ToArray();
            string collection = where[0];
            string whereClause = where[1];

            string js = @"db." + collection + ".find( {" + whereQuery(whereClause) + "}" + (select.Length > 0 ? (", " + select) : "" ) + ");";
            return js;
        }

        public string insert(string insertStatement)
        {
            string[] blocks = insertStatement.Replace('\n', ' ').Split(' ').Select(a => a.Trim()).Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
            string collection = blocks[2];

            List<string> p = new List<string>();
            int open = -1, close = -1;
            for (int i = 0; i < insertStatement.Length; i++)
            {
                if (insertStatement[i] == '(')
                    open = i;
                else if (insertStatement[i] == ')')
                    close = i;

                if(close > open)
                {
                    p.Add(insertStatement.Substring(open + 1, close - open - 1));
                    open = -1;
                    close = -1;
                }

            }

            if(p.Count < 2)
            {
                return "Not valid";
            }


            string json = "{ [";
            string[] columns = p[0].Split(',').Select(a => a.Trim()).ToArray();
            for (int i = 1; i < p.Count; i++)
            {
                json += "{";
                string[] values = p[i].Split(',').Select(a => a.Trim()).ToArray();
                for (int j = 0; j < columns.Length; j++)
                {
                    json += columns[j] + " : " + values[j] + (j == columns.Length - 1 ? "" : ", ");
                }
                json += "}" + (i == p.Count - 1 ? "" : ", \r\n");
            }

            
            json += "]}";

            string js = @"db." + collection + ".insertMany(" + json + ");";
            return js;
        }
        public string getEquivToken(string token)
        {
            if (Array.IndexOf(splitters.Keys.ToArray(), token) > -1)
            {
                if(token == "declare")
                {

                }
                return tokens[token];

            }
            return token;
        }

        public string handleDeclareSet(string line)
        {
            string js = "";
            if (line.StartsWith("declare")) js = "var ";
            //List<string> variables = new List<string>();
            string[] decl = line.Split(new char[] { ',' }).Select(a => a.Trim()).Where(a => a != "").ToArray();
            for (int i = 0; i < decl.Length; i++)
            {
                string varWord = decl[i].Split('@').Select(a => a.Trim()).ToArray()[1];
                string[] perfectvarWord = varWord.Split(new string[] { "=" }, StringSplitOptions.None).Select(a => a.Trim()).ToArray();
                if (perfectvarWord.Length == 1)
                {
                    //variables.Add(perfectvarWord[0].Split(' ')[0]);
                    js += perfectvarWord[0].Split(' ')[0];
                }
                else
                {
                    //variables.Add(perfectvarWord[0].Split(' ')[0] + "=" + perfectvarWord[1]);
                    js += perfectvarWord[0].Split(' ')[0] + " = " + perfectvarWord[1];
                }
                //variables.Add(decl[i].Substring(1));
                if (i != decl.Length - 1) js += ", ";
            }
            return js;
        }
        public string nativeJS(string line)
        {
            string js = "";
            if(line.StartsWith("declare") || line.StartsWith("set"))
            {
                js = handleDeclareSet(line);
            } else if(line.StartsWith("if"))
            {
                js = "if" + conditional(line.Trim().Substring(2)) + "";
                foreach (string key in fills.Keys.ToArray())
                {
                    js = js.Replace(key, fills[key]);
                }
                fills = new Dictionary<string, string>();
            }
            //string[] words = line.Split(' ').Select(a => a.Trim()).Where(a => a != "").ToArray();
            //foreach (string word in words)
            //{
            //    js += getEquivToken(word) + " ";
            //}
            return js;
        }
        public List<string> segreggator(string[] blocks)
        {
            List<string> jsLines = new List<string>();
            foreach (string block in blocks)
            {
                string[] newlines = block.Split('\n');
                foreach(string newline in newlines)
                {
                    if (newline.ToLower().StartsWith("delete"))
                    {
                        jsLines.Add(delete(newline));
                    }
                    else if (newline.ToLower().StartsWith("insert"))
                    {
                        jsLines.Add(insert(newline));
                    }
                    else if (newline.ToLower().StartsWith("update"))
                    {
                        jsLines.Add(update(newline));
                    }
                    else if (newline.ToLower().StartsWith("select"))
                    {
                        jsLines.Add(select(newline));
                    } else
                    {
                        jsLines.Add(nativeJS(newline));
                    }
                }
            }
            return jsLines;
        }

        public string[] getBlocks(string storedProc)
        {
            return storedProc.ToLower().Split(new string[] { "begin", "end" }, StringSplitOptions.None).Skip(1).Select(block => block.Trim()).ToArray<string>();
        }

        public List<Param> getParams(string storedProc)
        {
            string paramText = storedProc.Split('(')[1].Split(')')[0];
            string[] almostParams = paramText.Split(',').Select(value => value.Trim().Substring(1)).ToArray();
            List<Param> p = new List<Param>();
            for (int i = 0; i < almostParams.Length; i++)
            {
                string[] components = almostParams[i].Split(' ').Select(value => value.Trim()).ToArray();
                Param param = new Param();
                param.Name = components[0];
                param.DataType = components[1];
                if (components.Length > 2)
                {
                    param.Type = components[2];
                }

                p.Add(param);
            }
            return p;
        }

        public string getProcName(string storedProc)
        {
            var words = storedProc.Split(' ');
            for (int i = 0; i < words.Length; i++)
            {
                if (words[i].ToLower() == "procedure")
                {
                    return words[i + 1].Split('\n')[0].Split('(')[0];
                }

            }
            return null;
        }
    }
}
