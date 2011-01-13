## depend on JSON and HTTPClient
require 'rubygems'
require 'ruby-debug'

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
    
    clnt = HTTPClient.new
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
  
  def save
    if @data["id"]
    else
      clnt = HTTPClient.new
      @data["root_doc_name"] = self.class.to_s
      @data["date"] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
      rs = clnt.put("#{self.couchdb_url}/#{self.uuid}",JSON.generate(@data))
      self.data.merge!(JSON.parse(rs.content))
      return self.data["ok"]
    end    
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
  
  def self.create_view(view_name, function)
    views_url = "#{self.new.couchdb_url}/_design/#{self.new.couchdb_db_name}_views"
    
    clnt = HTTPClient.new
    response = clnt.get("#{views_url}/_view/#{view_name}")
    
    if JSON.parse(response.content)["error"] == "not_found"
      json_hash = {
        "language" => "javascript",
        "views" => {
          "#{view_name}" => {
            "map" => "#{function.split.join(' ')}"
          }
        }
      }

      response = clnt.put(views_url,JSON.generate(json_hash))
      result_hash = JSON.parse(response.content)
    
      if result_hash["ok"] == true
        response = clnt.get("#{views_url}/_view/#{view_name}")
        return self.parse(response.content)
      else
        return []
      end   
    else
      return self.parse(response.content)
    end    
      
  end
    
  def uuid
    clnt = HTTPClient.new
    rs = clnt.get("#{self.couchdb_server}_uuids")
    return JSON.parse(rs.content)["uuids"]
  end  
end  