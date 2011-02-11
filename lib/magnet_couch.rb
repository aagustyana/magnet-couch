## depend on JSON and HTTPClient
require 'rubygems'
require 'ruby-debug'

class String
  def blank?
    if self.nil? or self.empty?
      return true
    else
      return false
    end    
  end
end    

class MagnetCouch
  ## data contains key-value data that will store in couchdb
  ## couchdb_server : couchdb server url, default = http://localhost:5984/
  ## couchdb_db name : couchdb database name, default = couuchdb_db_name
  ## couchdb_url : couchdb complete url
  
  attr_accessor :data, :couchdb_server, :couchdb_url, :couchdb_db_name, :_id, :_rev, :errors
  
  def initialize(data={}, server=nil, db_name = nil)
    @data = data
    @_id = data["_id"]
    @_rev = data["_rev"]
    @errors = []
    
    begin
      @couchdb_server = COUCHDB_SERVER
    rescue NameError
      @couchdb_server = (server.nil?) ? "http://localhost:5984/" : server
    end    
    
    begin
      @couchdb_db_name = COUCHDB_DB_NAME
    rescue NameError
      @couchdb_db_name = (db_name.nil?) ? "magnet_couch" : db_name
    end    
    
    @couchdb_url = "#{@couchdb_server}#{@couchdb_db_name}"
    
  end
  
  def self.find(id)
    clnt = HTTPClient.new
    response = clnt.get("#{self.new.couchdb_url}/#{id}")
    result_hash = JSON.parse(response.content)
    self.new(result_hash)
  end  
  
  def update_attributes(params)
    
    self.data.each do |key,value|
      self.data[key] = params[key] if params.keys.include?(key)
    end  
    
    before_save
    
    clnt = HTTPClient.new
    
    self.data["updated_at"] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
    
    response = clnt.put("#{self.couchdb_url}/#{self._id}",JSON.generate(self.data.reject {|key,value| key=="_id"}))
    result_hash = JSON.parse(response.content)
    
    if result_hash["error"]
      self.errors << result_hash["reason"]
      return false
    elsif result_hash["rev"]
      self._rev = self.data["_rev"] = result_hash["rev"] 
      return true
    else
      return false
    end    
  end  
  
  def destroy
    clnt = HTTPClient.new
    response = clnt.delete("#{self.couchdb_url}/#{self._id}?rev=#{self._rev}")
    result_hash = JSON.parse(response.content)
  
    if result_hash["error"]
      self.errors << result_hash["reason"]
      return false
    else
      return true
    end      
  end  
  
  ### it's supposed to be callback
  def before_save
  end  
  
  def save
    before_save
    
    if self._id
      self.update_attributes(self.data)
    else
      clnt = HTTPClient.new
      self.data["doc_type"] = self.class.to_s
      self.data["created_at"] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
      self.data["updated_at"] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
      
      response = clnt.post("#{self.couchdb_url}",JSON.generate(self.data), {'Content-Type' => 'application/json'})
      result_hash = JSON.parse(response.content)
      
      if result_hash["error"]
        self.errors << result_hash["reason"]
        return false
      else
        self._id = self.data["_id"] = result_hash["id"]
        self._rev = self.data["_rev"] = result_hash["rev"]
        return true
      end    
    end    
  end
  
  def add(key, value)
    self.data[key] = [] if self.data[key].nil?
    self.data[key] << value.merge(:_id => uuid)  
  end  
  
  ### still inefficient, need to be replaced by views
  def replace(key,value,sub_id)
    rs = false
    if self.data[key]
      self.data[key].each_with_index do |arr,idx|
        if arr["_id"][0].to_s == sub_id.to_s
          self.data[key][idx] = value.merge(:_id => sub_id)
          rs = true
          break
        end  
      end  
      
    else
      @errors << "can't update due to missing data #{sub_id}"   
    end
        
    return rs
  end  
  
  def remove(key,id)
    self.data[key] == self.data[key].delete_if {|sub| sub["_id"].to_s == id.to_s}
  end  
  
  ### still inefficient, need to be replaced by views
  def emit_sub_doc(key,id)
    rs = nil
    
    if self.data[key]
      self.data[key].each do |arr|
        if arr["_id"].to_s == id.to_s
          rs = MagnetCouch.new(arr)
          break
        end  
      end  
    end
    return rs
      
  end  
  
  def self.parse(json)
    mc_datas = []
    
    json_hash = JSON.parse(json)
    if json_hash["rows"]
      json_hash["rows"].each do |row|
        mc_datas << self.new(row["value"])
      end  
    end
    return mc_datas          
  end  

  def self.find_all
    function = <<-eos
      function(doc) {
        if (doc.created_at && doc.doc_type == '#{self.new.class}' ) {
          emit(doc._id,doc);
        }
      }
    eos
    
    return self.create_view_and_get_result("find_all",function)
    
  end  
  
  ### options[:http_params], this is a query string
  ### function contains map & reduce if it's hash
  ### if function contains string it's map only
  
  def self.create_view_and_get_result(view_name, function, options= {})
    clnt = HTTPClient.new
    response = clnt.get self.design_path(view_name)
    
    if JSON.parse(response.content)["error"] == "not_found"
      
      result_hash = self.create_view(view_name, function)
      response = clnt.get self.design_path(view_name)
    end
    
    response = clnt.get self.view_path(view_name, options)  
    return self.parse(response.content)
  end  
  
  def self.design_path(view_name)
    "#{self.new.couchdb_url}/_design/#{self.new.class.to_s}_#{view_name}_view"
  end  
  
  def self.view_path(view_name, options = {})
    @qry_string = "?#{options[:http_params]}" unless options[:http_params].blank?
    return "#{self.design_path view_name}/_view/#{view_name}#{@qry_string}"
  end  
  
  ## required couchdb-lucene
  def self.create_lucene_view(view_name,json_hash) 
    
    clnt = HTTPClient.new
    response = clnt.put(self.design_path(view_name),JSON.generate(json_hash))    
    
    return JSON.parse(response.content)
  end
  
  def self.lucene_view_path(view_name)
    #http://localhost:5984/db_name/_fti/_design/View_name/multiple_keys?q"
    return "#{self.new.couchdb_url}/_fti/_design/#{self.new.class.to_s}_#{view_name}_view"
  end
  
  def self.create_view(view_name, function)
    if function.class == String
      map = function
      reduce = nil
    elsif function.class == Hash
      map = function[:map]
      reduce = function[:reduce]  
    end  
    
    json_hash = {
      "language" => "javascript",
      "views" => {
        "#{view_name}" => {
          "map" => "#{map.split.join(' ')}",
          "reduce" => "#{reduce.split.join(' ')}",
        }
      }
    }

    clnt = HTTPClient.new
    response = clnt.put(self.design_path(view_name),JSON.generate(json_hash))    
    
    return JSON.parse(response.content)
         
  end
  
  def self.create_view_and_get_keys(view_name, function, options= {})
    clnt = HTTPClient.new
    response = clnt.get self.design_path(view_name)
    
    if JSON.parse(response.content)["error"] == "not_found"
      
      result_hash = self.create_view(view_name, function)
      response = clnt.get self.design_path(view_name)
    end
    
    response = clnt.get self.view_path(view_name, options)  
    rows = JSON.parse(response.content)["rows"] 
    
    rs = []
    if rows
      rows.each do |row|
        rs << row["key"] if row["key"]
      end 
    end
    
    return rs
  end  
  
    
  def uuid
    clnt = HTTPClient.new
    rs = clnt.get("#{self.couchdb_server}_uuids")
    return JSON.parse(rs.content)["uuids"]
  end 
  
end  