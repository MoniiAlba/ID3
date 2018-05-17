/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package id3;


import static java.lang.Integer.parseInt;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.swing.DefaultListModel;

/**
 *
 * @author soeur
 */
public class Tabla {
    
    public int totCols;
    public int totTuplas;
    public int colMinEnt;
    public double totEnt;
    public double sumaProbs;
    public int columnaObjetivo;
    public int columnaId;
    public String nombre;           //NOMBRE DE LA TABLA NODO
    public String nomColObj;
    public String nomDataBase;
    public String nomTable;
    public String atrPrincipal;     //NOMBRE DE LA COLUMNA CON MENOR ENTROPÍA
    public String valTerminal;      //VALOR DE COLUMNA OBJETIVO DE LA TABLA TERMINAL
    public Connection conexion;
    public Tabla papa;              //PAPÁ TABLA NODO
    public Map<Integer,String> columnas = new HashMap<>();
    public Map<Integer,String> columnasRec;
    public Map<String,Tabla> subtablas = new HashMap<>(); //valor del padre y tabla  HIJOS
    //public ArrayList<Map<Integer,String>> valsDist;
    public Map<String,Map<Integer,String>> valsDist;
    public DefaultListModel listaEnt;
    

      
    //CONSTRUCTOR TABLA PADRE
    public Tabla(Tabla pa, String n, String nBD, String nT, String obj, String id, Connection c) throws SQLException{
        papa = pa;
        nombre = n;
        nomDataBase = nBD;
        nomTable = nT;        
        conexion = c;     
        nomColObj = obj;
        dameDatosTabla();
        columnaObjetivo = dameLlave(obj);
        columnaId = dameLlave(id);
        System.out.println("Columna objetivo: "+columnaObjetivo);
        System.out.println("Columna id: "+columnaId);
    }
    
    public int dameLlave(String str){
        int cont = 0;
        while(!columnas.get(cont).equals(str)){
            cont++;
        }
        return cont;
    }
    
    
    public void dameDatosTabla() throws SQLException{
        if(papa!=null){
            System.out.println("Tabla nueva, nombre: "+nombre+" Padre: "+papa.nombre );
            getColsH();
        }else{
            System.out.println("Tabla nueva, nombre: "+nombre);
            getCols();
        }
        
        
        totCols = columnas.size();
        valsDist = new HashMap<>();
        
    }
    
    public void dameDatosNoClas() throws SQLException{
        totTuplas = getTotal();        
        getValsDist();
        System.out.println("Tamaño valsDist: "+valsDist.size());
        System.out.println("Valores distintos tabla: "+valsDist.toString());
        entropia();
        System.out.println("Columna con menor entropía: "+colMinEnt+" = " +columnas.get(colMinEnt));
        atrPrincipal = columnas.get(colMinEnt);
        System.out.println("Columna menor entropía: "+atrPrincipal);
        System.out.println("Valores distintos col menor entropia: "+imprimeValsCol(atrPrincipal));
        System.out.println(toString());
    }
    
    public void cierraRs(ResultSet rs){
        try{
            rs.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
    
    public ResultSet getAlgo(String st) throws SQLException{
        Statement sqlStatement = conexion.createStatement();
        ResultSet rs = null;
        String queryString=st;
        //System.out.println("\nQuery string:");
        //System.out.println(queryString);
        rs=sqlStatement.executeQuery(queryString);
        return rs;  
        
    }
    
    public ResultSet getAlgo(Connection c, String st) throws SQLException{
        Statement sqlStatement = c.createStatement();
        ResultSet rs = null;
        String queryString=st;
        System.out.println("\nQuery string:");
        System.out.println(queryString);
        rs=sqlStatement.executeQuery(queryString);
        return rs;  
        
    }
    
    public void getCols() throws SQLException {
        String query = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = '"+nomDataBase+"' AND  TABLE_SCHEMA='dbo' AND TABLE_NAME ='"+nomTable+"'";
        ResultSet rs = getAlgo(query);
        int cont = 0;
        while(rs.next()){
            System.out.println(rs.getString("COLUMN_NAME"));
           columnas.put(cont,rs.getString("COLUMN_NAME"));
           cont++;
        }
        System.out.println("Columnas de la tabla: \n"+columnas.toString());
        cierraRs(rs);
    }
    
    public void getColsH() throws SQLException {
        String query = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name like '"+nombre+"%'";
        ResultSet rs = getAlgo(query);
        int cont = 0;
        while(rs.next()){
            System.out.println(rs.getString("COLUMN_NAME"));
            columnas.put(cont,rs.getString("COLUMN_NAME"));
            cont++;
        }
        System.out.println("Columnas de la tabla: \n"+columnas.toString());
        cierraRs(rs);
    }
    
      public int getTotal() throws SQLException{
      ResultSet rs = getAlgo("Select count ("+columnas.get(0)+") as tot FROM "+nombre);
      String resp = "";
      while(rs.next()){
           resp = rs.getString("tot");
      }
      cierraRs(rs);
      return parseInt(resp);
  }
    
      /*
    public void getValoresDistintos() throws SQLException{
        for(int i = 1; i < totCols; i ++){
            Map<Integer,String> aux = new HashMap<>();  
            int cont = 0;
            //String query = "select distinct "+columnas.get(i)+" as valores from ["+nomDataBase+"].[dbo].["+nomTable+"]";
            String query = "select distinct "+columnas.get(i)+" as valores from "+nombre;
            ResultSet rs = getAlgo(query);
            while(rs.next()){
                aux.put(cont, rs.getString("valores").toString());
                cont++;
            }
            valsDist.add(aux);
            cierraRs(rs);
        }
        
        
    }
      */
    
    public void getValsDist() throws SQLException{
        for(Map.Entry<Integer,String> entry : columnas.entrySet()){
            int cont = 0;
            Map<Integer,String> aux = new HashMap<>();
            if(entry.getKey()!=columnaId ){
                String query = "select distinct "+columnas.get(entry.getKey())+" as valores from "+nombre;
                ResultSet rs = getAlgo(query);
                while(rs.next()){
                    aux.put(cont, rs.getString("valores").toString());
                    cont++;
                }
                valsDist.put(entry.getValue(),aux);
            }         
        }
    }
    
    public double getValoresEntropia(int col, String c) throws SQLException{ //columna de "columnas"
        System.out.println("Columna: "+columnas.get(col));
        System.out.println("Distintos valores: "+valsDist.get(c)+"; tamaño: "+valsDist.get(c).size());
        totEnt = 0;
        double sumaParcial = 0;
        
        int veces = valsDist.get(c).size();
        for(int i = 0; i < veces; i++){ //valores distintos de la columna COL
            String query = "Select \""+columnas.get(col)+"\" AS COL, count(\""+columnas.get(col)+"\") as cuenta FROM "+nombre+" WHERE \""+columnas.get(col)+"\" = '"+valsDist.get(c).get(i) +"' GROUP BY \""+columnas.get(col)+"\"";
            ResultSet rs = getAlgo(query);
            double repeticion = 0;
            while(rs.next()){
                repeticion = parseInt(rs.getString("cuenta"));
                System.out.println("Repeticion valor "+valsDist.get(c).get(i)+": "+repeticion);
            }
            sumaProbs = 0;
            double cons = repeticion / totTuplas;
            if( repeticion != 0){
                int veces2 = valsDist.get(nomColObj).size(); //valores distintos de la columna objetivo
                System.out.println("Valores distintos de columna objetivo: "+valsDist.get(nomColObj));
                for(int j = 0; j < veces2; j ++){
                    //obtiene la cuenta del valor i de la columna col dado el valor j de la columna objetivo
                    query = "Select \""+columnas.get(col)+"\" AS COL, count(\""+columnas.get(col)+"\") as cuenta FROM "+nombre+"  WHERE \""+columnas.get(col)+"\" = '"+valsDist.get(c).get(i) +"' AND \""+columnas.get(columnaObjetivo)+"\" = '"+valsDist.get(nomColObj).get(j)+"' GROUP BY \""+columnas.get(col)+"\"";
                    ResultSet rq = getAlgo(query);
                    double repDado = 0;
                    while(rq.next()){
                        repDado = parseInt(rq.getString("cuenta"));
                        System.out.println("Repeticion valor "+i+" dado valor "+j+" de col obj: "+repDado);
                    }

                    double fracc = repDado / repeticion;

                    //System.out.println("Fracc= repDado "+repDado+" / repeticion "+repeticion+" = "+fracc);
                    if(fracc!=0){
                        sumaParcial = fracc * (Math.log(fracc)/Math.log(2));
                        sumaProbs+= sumaParcial;
                        //System.out.println("Suma de probas: "+sumaProbs);
                    }
                    cierraRs(rq);
                }
            }
            //System.out.println("Suma parcial : "+sumaParcial);
            if(sumaParcial!=0){
                totEnt += cons * (-sumaProbs);
            }          
            //System.out.println("Tot Entropia: "+totEnt);
            cierraRs(rs);
            
        }      
        System.out.println("Entropia de col "+columnas.get(col)+": "+totEnt);      
        return totEnt;
    }
  
    /*
    public int getEntropia() throws SQLException{
        listaEnt = new DefaultListModel();
        double minEnt = 100;
        int colMinEnt = -1;
        for(int i = 0; i < totCols; i ++){
            if(i == columnaId){
                i++;
            }
            if(i != columnaObjetivo ){
                double ent = getValoresEntropia(i);
                System.out.println("Entropia de col "+columnas.get(i)+": "+ent);
                listaEnt.addElement("Entropia de col "+columnas.get(i)+": "+ent);
                if(ent < minEnt){
                    minEnt = ent;
                    colMinEnt = i;
                }      
                    
            }
                
        }
        return colMinEnt;// regresamos la columna contando que 0 es la columna del id
    }
    */
    
    public int entropia() throws SQLException{
        listaEnt = new DefaultListModel();
        double minEnt = 10000;
        int colMin = -1;
        System.out.println("Columnas: "+columnas.toString());
        for(Map.Entry<Integer,String> entry : columnas.entrySet()){
            double ent;
            System.out.println("Llave: "+entry.getKey()+" Valor de columna: "+entry.getValue());
            if(entry.getKey()!=columnaId && entry.getKey() != columnaObjetivo){
                ent = getValoresEntropia(entry.getKey(), entry.getValue());
                listaEnt.addElement("Entropia de col "+entry.getValue()+": "+ent);
                if(ent < minEnt){
                    minEnt = ent;
                    colMin = entry.getKey();
                }
            }
            
        }
        colMinEnt = colMin;
        return colMin;
    }
    
    public void colsMenos(String col){
        columnasRec = new HashMap<>();
        for(Map.Entry<Integer,String> entry : columnas.entrySet()){
            if(!entry.getValue().equals(col)){
                columnasRec.put(entry.getKey(),entry.getValue());
            }        
        }        
    }
  
    public boolean yaClasificado() throws SQLException{ 
        String query = "select count (distinct \""+columnas.get(columnaObjetivo)+"\") as numVal from "+nombre;
        ResultSet rs = getAlgo(query);
        int tot = 0;
        while(rs.next()){
            tot = parseInt(rs.getString("numVal"));
        }
        if(tot < 2){
            String q = "select \""+columnas.get(columnaObjetivo)+"\" as val from "+nombre+" group by \""+columnas.get(columnaObjetivo)+"\"";
            ResultSet r = getAlgo(q);
            
            while(r.next()){
                valTerminal = r.getString("val");
            }
            return true;
        }else{
            return false;
        }
    }
    
    public void imprimeCols(int numCols, int estaNo){
        for(int i = 0; i < numCols; i++ ){
            if(i != estaNo){
                System.out.print(columnas.get(i) + " --------------- ");
            }
        }
        System.out.println();
    }
        
    public ArrayList<Tabla> generaHijos(int contG) throws SQLException{
        
        ArrayList<Tabla> hijos = new ArrayList<>();  //ARREGLO HIJOS
        int cont = 1;
        boolean sigue = true;
        while(sigue){
            colsMenos(columnas.get(colMinEnt));    
            System.out.println("Columnas menos colMinEnt: "+columnasRec.toString());
            String query = "";
            /*
            if(papa != null){
                query = "SELECT * INTO TempTable"+papa.nombre+atrPrincipal+cont+" FROM "+nombre+" WHERE \""+columnas.get(colMinEnt)+"\" = '"+valsDist.get(colMinEnt-1).get(cont-1)+"' ALTER TABLE TempTable"+papa.nombre+atrPrincipal+cont+" DROP COLUMN \""+columnas.get(colMinEnt)+"\" SELECT * FROM TempTable"+papa.nombre+atrPrincipal+cont;
            }else{*/
                query = "SELECT * INTO TempTable"+nombre+atrPrincipal+contG+" FROM "+nombre+" WHERE \""+columnas.get(colMinEnt)+"\" = '"+valsDist.get(columnas.get(colMinEnt)).get(cont-1)+"' ALTER TABLE TempTable"+nombre+atrPrincipal+contG+" DROP COLUMN \""+columnas.get(colMinEnt)+"\" SELECT * FROM TempTable"+nombre+atrPrincipal+contG;
            //}
            
            ResultSet rq = getAlgo(query);
            //imprimeCols(totCols, colMinEnt);
            while(rq.next()){
                for(Map.Entry<Integer,String> entry : columnasRec.entrySet()){
                    String val = rq.getString(columnasRec.get(entry.getKey()));
                    System.out.print(val+ " --------- ");
                }
                System.out.println();
            }
            cierraRs(rq);
            Tabla hijo = null;
            /*
            if(papa!=null){
                hijo = new Tabla(this,"TempTable"+papa.nombre+atrPrincipal+cont, nomDataBase, nomTable, columnas.get(columnaObjetivo), columnas.get(columnaId), conexion);//al hijo le pego una referencia al papa (this)
            }else{ */
                hijo = new Tabla(this,"TempTable"+nombre+atrPrincipal+contG, nomDataBase, nomTable, columnas.get(columnaObjetivo), columnas.get(columnaId), conexion);//al hijo le pego una referencia al papa (this)
            //}        
            subtablas.put(valsDist.get(columnas.get(colMinEnt)).get(cont), hijo);
            hijos.add(hijo);
            cont++;
            contG++;
            sigue = cont <= valsDist.get(columnas.get(colMinEnt)).size();
        }
        return hijos;
    }
    
    @Override
    public String toString(){
        return "Atributo principal: "+atrPrincipal+"; Nombre tabla: "+nombre+"; Hijos: "+subtablas.toString()+"; ColMinEnt: "+ colMinEnt + "; ColObjetivo: "+
                columnaObjetivo + "; lista de entropías: "+ listaEnt.toString() + "; total de columnas: "+ totCols+"; numero de tuplas"+totTuplas;
    }

    private String imprimeValsCol(String col) {
        return valsDist.get(col).toString();
    }
  
}
