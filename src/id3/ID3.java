package id3;

import static java.lang.Integer.parseInt;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.DefaultListModel;
import static javax.xml.bind.DatatypeConverter.parseInteger;

/**
* Connect to SQL Server, execute a SELECT query, print the results.
*
*/  
public class ID3 {

    
    private String columnaObjetivo;
    private String columnaId;
    public int colMinEnt;
    public static Connection conexion;
    public static String nomDataBase;
    public static String nomTable;
    public DefaultListModel listaEnt;
    //public Tabla padre;
    
    
    public ID3(String nomB, String nomT, String colObj, String colId) throws SQLException{        
        nomDataBase = nomB;
        nomTable = nomT;      
        columnaObjetivo = colObj;
        columnaId = colId;
        
        correTodo();
        
    }
    

    
    //The SQL Server JDBC Driver is in 
    //C:\Program Files\Microsoft JDBC Driver 6.0 for SQL Server\sqljdbc_6.0\enu\auth\x64
    private static final String jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    //The JDBC connection URL which allows for Windows authentication is defined below.
    private static String jdbcURL;
    //To make Windows authentication work we have to set the path to sqljdbc_auth.dll at the command line

    /**
    * main method.
    *
    * @param  args  command line arguments
    */  
    public static void main(String[] args) throws SQLException{
        ID3 a = new ID3("Cancer", "cancercito","Class","Sample_code_number");
        
        //a.imprimeReglas(a.padre);
        /*
        System.out.println(nomDataBase);
        jdbcURL = "jdbc:sqlserver://localhost;user=sa;password=azmariachan;databasename="+nomDataBase+";";
        a.conexion = conecta();
        Tabla padre = new Tabla(null,"[cancercito]", a.nomDataBase, a.nomTable, a.columnaObjetivo, a.columnaId, conexion);
        a.id3(padre);
        a.cierraConec(conexion);
        /*
          a.getCols();
        a.totCols = columnas.size();
        System.out.println("Total de columnas: "+a.totCols);
        a.totalTuplas = a.getTotal();
        System.out.println("Total de tuplas: "+a.totalTuplas);
        //Sigue obtener valores distintos por cada columna
        a.valsDist = new ArrayList<Map<Integer,String>>(a.totCols-1);
        a.getValoresDistintos();
        System.out.println(a.valsDist.toString());
        //Obtener valores entropía
        a.colMinEnt = a.getEntropia();
        System.out.println("Columna con menor entropia: "+columnas.get(a.colMinEnt));
        System.out.println("Tabla recortada: ");
        a.obtenHijos();
    */

    }
  
    public void correTodo() throws SQLException{
        System.out.println(nomDataBase);
        jdbcURL = "jdbc:sqlserver://localhost;user=sa;password=azmariachan;databasename="+nomDataBase+";";
        conexion = conecta();
        Tabla padre = new Tabla(null,"cancercito", nomDataBase, nomTable, columnaObjetivo, columnaId, conexion);
        System.out.println(padre.columnas);
        id3(padre);

    }
  
    public static Connection conecta(){
        System.out.println("Program started");
        try{
            Class.forName(jdbcDriver).newInstance();
            System.out.println("JDBC driver loaded");
        }catch (Exception err){
            System.err.println("Error loading JDBC driver");
            err.printStackTrace(System.err);
            System.exit(0);
        }
    
        Connection databaseConnection= null;
        try{
            //Connect to the database
            databaseConnection = DriverManager.getConnection(jdbcURL);
            System.out.println("Connected to the database");
            return databaseConnection;
        }catch (SQLException err){
            System.err.println("Error connecting to the database");
            err.printStackTrace(System.err);
            System.exit(0);
            return null;
        }
    }
  
  
    public void cierraConec(Connection c) throws SQLException{      
        c.close();      
        System.out.println("Database connection closed");
    }
  
    public void cierraRs(ResultSet rs){
        try{
            rs.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }

    int cont = 0;
    public void id3(Tabla tIni) throws SQLException{
        if(!tIni.yaClasificado()){
            System.out.println(" ---- NO SOY TERMINAL ----");;
            tIni.dameDatosNoClas();
            
            for(Tabla t : tIni.generaHijos(cont)){
                id3(t);
                cont+=tIni.subtablas.size();
            }
        }else{
            System.out.println("---- SOY TERMINAL -----");
        }
    }
    
    public void imprimeReglas(Tabla t){
        System.out.println(""+ t.atrPrincipal);
        
        for(Map.Entry<String,Tabla> entry : t.subtablas.entrySet()){
            System.out.print(" = "+ entry);
            imprimeReglas(entry.getValue());
        }
    }
  
/*
  
    public void imprimeCols(int numCols, int estaNo){
        for(int i = 0; i < numCols; i++ ){
            if(i != estaNo){
                System.out.print(columnas.get(i) + " --------------- ");
            }
        }
        System.out.println();
    }
    

  
    public boolean sigue = true;
    public void obtenHijos() throws SQLException{
        int cont = 0;
        
        while(sigue){
            String query = "SELECT * INTO #TempTable"+cont+" FROM [Cancer].[dbo].[cancercito] WHERE \""+columnas.get(colMinEnt)+"\" = '"+valsDist.get(colMinEnt).get(cont)+"' ALTER TABLE #TempTable"+cont+" DROP COLUMN \""+columnas.get(colMinEnt)+"\" SELECT * FROM #TempTable"+cont;
            ResultSet rs = getAlgo(conexion,query);
            imprimeCols(totCols, colMinEnt);
            colsMenos(colMinEnt);
            while(rs.next()){
                for(Map.Entry<Integer,String> entry : columnasRec.entrySet()){
                    String val = rs.getString(columnasRec.get(entry.getKey()));
                    System.out.print(val+ " --------- ");
                }
                System.out.println();
            }
            cont++;
            String queryColOb = "";
            sigue = cont < valsDist.get(colMinEnt).size();
        }
    }
*/
}
