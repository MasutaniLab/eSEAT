#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''

RtcSh
Copyright (C) 2018
    Isao Hara,AIST,Japan
    All rights reserved.
  Licensed under the MIT License (MIT)
  http://www.opensource.org/licenses/MIT
'''

############### import libraries
from __future__ import print_function
import sys
import os
import time
import signal
import re
import traceback
import subprocess
import platform
import threading
import socket
import signal

try:
  import readline
except:
  pass
  
import cmd

#########################
from RTC import *
import CosNaming 
from CorbaNaming import *
import SDOPackage
from omniORB import CORBA,URI

import OpenRTM_aist
import string

import OpenRTM_aist.CORBA_RTCUtil
import RTC, RTC__POA

import io
try:
  from PIL import Image
  import pydot
except:
  PIL = None

sys.path.append(".")
sys.path.append("rtm")

import OpenRTM_aist.version

##### rtm version
def rtm_version():
    ver=OpenRTM_aist.version.openrtm_version.split(".")
    return int(ver[0])*10000 + int(ver[1]) * 100 + int(ver[2]) 

#######  T E S T ###############
def showGraph(graph):
    #graph.write_png('rtcmd.png')
    img = Image.open(io.BytesIO(graph.create_png()))
    img.show()


#####################################################
#
def encodeStr(data):
  return data.encode().decode('unicode_escape')

def decodeStr(data):
  return data.encode('raw-unicode-escape').decode('utf-8', 'backslashreplace')

def check_process(name):
    if platform.system() == "Windows":
        name=os.path.splitext(name)[0] + ".exe"
        proc=subprocess.Popen('tasklist', shell=True, stdout=subprocess.PIPE)
        for line in proc.stdout:
            if line.decode('shift-jis').startswith(name):
                
                return True
    print("===", name, "is not running.")
    return False
#
#
def convertDataType(dtype, data, code='utf-8'):
  if dtype == str:
    if sys.version_info.major == 2:
        return data.encode(code)
    else:
        return data

  elif sys.version_info.major == 2 and dtype == unicode:
    return unicode(data)

  elif dtype == int or dtype == float :
    return dtype(data)
  else:
    if type(data) == str :
        return eval(data)
    return data
#
#
def instantiateDataType(dtype):
  if isinstance(dtype, int) : desc = [dtype]
  elif isinstance(dtype, tuple) : desc = dtype
  else :  desc=omniORB.findType(dtype._NP_RepositoryId) 

  if desc[0] in [ omniORB.tcInternal.tv_alias ]: return instantiateDataType(desc[2])

  if desc[0] in [ omniORB.tcInternal.tv_short, omniORB.tcInternal.tv_long, 
                   omniORB.tcInternal.tv_ushort, omniORB.tcInternal.tv_ulong,
                   omniORB.tcInternal.tv_boolean, omniORB.tcInternal.tv_char,
                   omniORB.tcInternal.tv_octet, omniORB.tcInternal.tv_longlong,
                   omniORB.tcInternal.tv_enum
                  ]: return 0

  if desc[0] in [ omniORB.tcInternal.tv_float, omniORB.tcInternal.tv_double, omniORB.tcInternal.tv_longdouble ]: return 0.0

  if desc[0] in [ omniORB.tcInternal.tv_sequence, omniORB.tcInternal.tv_array,
                  ]: return []

  if desc[0] in [omniORB.tcInternal.tv_string ]: return ""
  if desc[0] in [omniORB.tcInternal.tv_wstring, omniORB.tcInternal.tv_wchar ]: return u""

  if desc[0] == omniORB.tcInternal.tv_struct:
    arg = []
    for i in  range(4, len(desc), 2):
      attr = desc[i]
      attr_type = desc[i+1]
      arg.append(instantiateDataType(attr_type))
    return desc[1](*arg)

  return None

def isDataInport(prof, dtype):
  try:
    return (prof['port.port_type'] == 'DataInPort' and (dtype == "" or prof['dataport.data_type'] == dtype))
  except:
    return True

def isDataOutport(prof, info):
  try:
    return (prof['port.port_type'] == 'DataOutPort')
  except:
    return True

def isServicePort(prof, info):
  try:
    return (prof['port.port_type'] == 'CorbaPort')
  except:
    return True

def format_connection(con):
  res=""
  res+=" %s\n" % con[0].connector_id
  res+="   name: %s\n" % con[0].name
  res+="   ports: %s\n" % con[0].ports
  res+="   properties: [\n" 
  for x in con[0].properties:
    res += "     %s: %s\n" % (x.name, x.value)
  res+="   ]\n" 
  return res
  
#########################################################################
# DataListener:  This class connected with DataInPort
#
class RtcDataListener(OpenRTM_aist.ConnectorDataListenerT):
    def __init__(self, name, type, obj):
        self._name = name
        self._type = type
        self._obj = obj
        self._ondata_thread=None
    
    def __call__(self, info, cdrdata):
      if rtm_version() < 20000:
        data = OpenRTM_aist.ConnectorDataListenerT.__call__(self,
                        info, cdrdata, instantiateDataType(self._type))
        self._obj.onData(self._name, data)
      else:
        self._obj.onData(self._name, cdrdata)
        
      return OpenRTM_aist.ConnectorListenerStatus.NO_CHANGE, data
      
###########################################################################
#
#
class Rtc_Sh:
  #
  #
  def __init__(self, orb=None, server_name='localhost'):
    if orb is None:
      self.orb = CORBA.ORB_init(sys.argv)
    else:
      self.orb=orb
    self.name=server_name
    self.naming=CorbaNaming(self.orb, self.name)
    self.maxlen=20
    self.object_list={}
    self.current_ctx=""
    self.manager = None
    #self.getRTObjectList()
    #######################################
    self.adaptors = {}
    self.adaptortype = {}
    self._data = {}
    self._port = {}

    self._consumer = {}
    self._ConsumerPort = {}
    self._ProviderPort = {}
    self._datatype = {}
    self._send_thread = None

    self._rtcmd=None
    self._update_thread = None
    ########################################

  #
  #
  def __del__(self):
    try:
      if self.manager:
        self.manager.shutdown()
      else:
        self.orb.shutdown(wait_for_completion=CORBA.FALSE)
        self.orb.destroy()
    except:
      pass

  def setRtcmd(self, obj):
    self._rtcmd=obj
  #
  #
  def initRtmManager(self):
    if self.orb :
      self.orb.shutdown(wait_for_completion=CORBA.FALSE)
      self.orb.destroy()

    argv=[sys.argv[0]]
    argv.append("-o")
    argv.append("logger.enable:NO")
    argv.append("-o")
    argv.append("manager.shutdown_on_nortcs: NO")
    #argv.append("-o")
    #argv.append("manager.is_master: YES")

    self.manager = OpenRTM_aist.Manager.init(argv)
    self.orb = self.manager._orb
    self.naming=CorbaNaming(self.orb, self.name)

    self.manager.activateManager()
    self.manager.runManager(True)

  #
  #
  def resolveRTObject(self, name):
    try:
      if name.count(".rtc") == 0 : name = name+".rtc"
      name = name.replace("%h", socket.gethostname())
      ref=self.naming.resolveStr(name)
      ref._non_existent()
      return ref._narrow(RTObject)
    except:
      #traceback.print_exc()
      return None

  def wait_for(self, name, timeout=0, func=None, flag=True):
    self.loop=True
    end_time=time.time() + timeout
    if func is None: func = self.resolveRTObject
    while self.loop :
      res=func(name)
      if (not res) == flag:
        if timeout > 0 and time.time() > end_time:
          self.loop=False
        time.sleep(0.3)
      else:
        return True
    return False
  #
  #
  def unbind(self, name):
    try:
      name = name.replace("%h", socket.gethostname())
      self.naming.unbind(name)
      print("Unbind :", name)
      return True
    except:
      return False

  #
  #
  def clearObjectList(self):
    self.object_list.clear()

  #
  #
  def getRTObjectList(self, name_context=None, parent=""):
    res=[]
    if name_context is None:
      name_context = self.naming._rootContext
    binds, bind_i = name_context.list(self.maxlen)
    for bind in binds:
      res = res + self.resolveBindings(bind, name_context, parent)
    if bind_i :
      tl = bind_i.next_n(self.maxlen)
      while tl[0]:
        for bind in tl[1] :
           res = res + self.resolveBindings(bind, name_conext, parent)
        tl = bind_i.next_n(self.maxlen)
    return res

  def getRTObjectNames(self, name_context=None, parent=""):
    lst=self.getRTObjectList(name_context, parent)
    return [ x[0] for x in lst]

  #
  #
  def resolveBindings(self, bind, name_context, parent):
    res = []
    prefix=parent

    if parent :
      prefix += "/"

    name = prefix + URI.nameToString(bind.binding_name)
    if bind.binding_type == CosNaming.nobject:
      if bind.binding_name[0].kind == "rtc":
        obj = name_context.resolve(bind.binding_name)
        try:
          obj._non_existent()
          obj = obj._narrow(RTObject)
          res = [[name, obj]]
          self.object_list[name] = obj
        except:
          obj = None
          res = [[name, obj]]
      else:
        pass
        #self.object_list[name] = None
    else:
      ctx = name_context.resolve(bind.binding_name)
      ctx = ctx._narrow(CosNaming.NamingContext)
      parent = name
      res = self.getRTObjectList( ctx, parent)
    return res

  #
  #
  def refreshObjectList(self):
    self.object_list.clear()
    return self.getRTObjectList()

  #
  #
  def getPorts(self, name, filter=None, f_arg=None):
    res=[]
    if name.count(".rtc") == 0 : name = name+".rtc"
    name = name.replace("%h", socket.gethostname())

    if not (name in self.object_list):
      self.refreshObjectList()

    if name in self.object_list:
      port_ref = self.object_list[name].get_ports()
      for p in port_ref:
        pp = p.get_port_profile()
        pprof =  nvlist2dict(pp.properties)

        if filter is None or filter(pprof, f_arg) :
          if pp.interfaces:
            ifp=pp.interfaces[0]
            pprof['interface_name'] = ifp.instance_name
            pprof['interface_type_name'] = ifp.type_name
            pprof['interface_polarity'] = ifp.polarity
          res.append( (pp.name, pprof))
    else:
      print("No such RTC:", name)
    return res

  #
  #
  def getPortRef(self, name, port):
    res=[]
    if name in self.object_list:
      self.refreshObjectList()

    if name.count(".rtc") == 0 : name = name+".rtc"
    name = name.replace("%h", socket.gethostname())

    if name in self.object_list:
      port_ref = self.object_list[name].get_ports()
      for p in port_ref:
        pp = p.get_port_profile()
        if port == pp.name.split('.')[-1]:
          return p
    else:
      print("No such port:", name, ":", port)
    return None

  #
  #
  def getPortDataType(self, name, port):
    res=[]
    if name.count(".rtc") == 0 : name = name+".rtc"
    name = name.replace("%h", socket.gethostname())

    if not (name in self.object_list):
      self.refreshObjectList()

    if name in self.object_list:
      port_ref = self.object_list[name].get_ports()
      for p in port_ref:
        pp = p.get_port_profile()
        pprof =  nvlist2dict(pp.properties)
        if pp.name.split(".")[1] == port:
          return pprof['dataport.data_type']
    else:
      print("No such RTC:", name)
    return None

  #
  #
  def getPortsInfo(self, name):
    res={}
    dport=[]
    sport=[]

    if name.count(".rtc") == 0 : name = name+".rtc"
    name = name.replace("%h", socket.gethostname())

    if not (name in self.object_list):
      self.refreshObjectList()

    if name in self.object_list:
      port_ref = self.object_list[name].get_ports()
      for p in port_ref:
        pinfo={}
        pp = p.get_port_profile()
        pinfo['name'] = pp.name.split('.')[-1]
        pprof =  nvlist2dict(pp.properties)

        if pprof['port.port_type'] == 'DataInPort' or pprof['port.port_type'] == 'DataOutPort' :
          pinfo['flow']=pprof['port.port_type']
          pinfo['type']=pprof['dataport.data_type'].split(':')[1].replace("/", "::")
          dport.append( pinfo )

        if pprof['port.port_type'] == 'CorbaPort' :
          inp=pp.interfaces[0]
          pinfo['if_name']=inp.instance_name
          pinfo['if_type_name']=inp.type_name
          if inp.polarity == REQUIRED:  pinfo['flow']= 'consumer'
          elif inp.polarity == PROVIDED:  pinfo['flow']= 'provider'
          else:  pinfo['flow']= 'unknown'
          sport.append(pinfo)
      res['dataport'] = dport
      res['serviceport'] = sport
    else:
      print("No such RTC:", name)
    return res

  #
  #
  def getConfigurationInfo(self, name):
    res=[]

    if name.count(".rtc") == 0 : name = name+".rtc"
    name = name.replace("%h", socket.gethostname())

    if not (name in self.object_list):
      self.refreshObjectList()

    if name in self.object_list:
      conf = self.object_list[name].get_configuration()
      conf_set = conf.get_configuration_sets()
      if conf_set:
        val={}
        const={}
        typ={}
        desc={}

        for cf in conf_set:
          data=cf.configuration_data
          if cf.id == "default":
            for x in data:  val[x.name] = x.value.value()
          elif cf.id == "__constraints__":
            for x in data:  const[x.name] = x.value.value()
          elif cf.id == "__type__":
            for x in data:  typ[x.name] = x.value.value()
          elif cf.id == "__description__":
            for x in data: desc[x.name] = x.value.value()
          else:
            pass

        for x in conf_set[0].configuration_data:
          data={}
          data['name'] = x.name
          
          if x.name in typ:  data['type'] = typ[x.name]
          else:  data['type'] = "string"
          
          if x.name in val: data['defaultValue'] = val[x.name]
          else:  data['defaultValue'] = ""

          if x.name in const:  data['constraints'] = const[x.name]
          else:  data['constraints'] = ""
          
          if x.name in desc:  data['description'] = desc[x.name]
          else:  data['description'] = ""
          res.append(data)
    else:
      print("No such RTC:", name)
    return res

  def getRtComponentInfo(self, name):
    res={}
    objref=self.resolveRTObject(name)
    prof=objref.get_component_profile()
    res=nvlist2dict(prof.properties)
    res['name'] = res['type_name']
    port_info=self.getPortsInfo(name)
    res['dataport'] = port_info['dataport']
    res['serviceport'] = port_info['serviceport']
    res['configuration'] = self.getConfigurationInfo(name)

    return res


  #
  #
  def getConnectors(self, name, port):
    port_ref=self.getPortRef(name, port)
    if port_ref:
      cons = port_ref.get_connector_profiles()
      return cons
    return None
 
  #
  #
  def getAllConnectors(self, name):
    res=[]
    if name.count(".rtc") == 0 : name = name+".rtc"
    port_ref = self.object_list[name].get_ports()
    for p in port_ref:
      res.append( p.get_connector_profiles() )
    return res

  def disconnectAll(self, name):
    try:
      connectors=self.getAllConnectors(name)
      for con in connectors:
        con.ports[0].disconnect(con.connector_id)
    except:
      print("Error in disconnectAll")
      return False
    return True

  def disconnectAllPorts(self):
    try:
      for name in self._port:
        connectors=self._port[name]._objref.get_connector_profiles()
        for con in connectors:
          con.ports[0].disconnect(con.connector_id)
    except:
      print("Error in disconnectAllPorts")
      return False
    return True
  
  #
  #
  def getConnectionInfo(self, con):
    ports = [(con.ports[0].get_port_profile()).name, (con.ports[1].get_port_profile()).name]
    res={'name': con.name, 'ports': ports, 'id': con.connector_id }
    return res

  #
  #
  def getConnections(self, name, port):
    res = []
    cons = self.getConnectors(name, port)
    if cons:
      for c in cons:
        res.append(self.getConnectionInfo(c))
    return res

  #
  #
  def find_connection(self, portname1, portname2):
    try:
      name1, port1 = portname1.split(":")
      name2, port2 = portname2.split(":")

      cons  = self.getConnectors(name1, port1)
      cons2 = self.getConnectors(name2, port2)
      if cons and  cons2 :
        for c in cons:
          for c2 in cons2:
            if c.connector_id == c2.connector_id:
              return c
      return False
    except:
      traceback.print_exc()
      return None

 #
  #
  def find_connections(self, portname1, portname2):
    try:
      name1, port1 = portname1.split(":")
      name2, port2 = portname2.split(":")
      res=[]
      cons  = self.getConnectors(name1, port1)
      cons2 = self.getConnectors(name2, port2)
      if cons and  cons2 :
        for c in cons:
          for c2 in cons2:
            if c.connector_id == c2.connector_id:
              res.append(c)
      return res
    except:
      traceback.print_exc()
      return None

  #
  #
  def connect(self, portname1, portname2, service=False):
    chk = self.find_connection(portname1, portname2)
    if chk is None:
        return None
    if chk :
       print("Conntction exists:", chk.connector_id)
       return chk.connector_id

    name1, port1 = portname1.split(":")
    name2, port2 = portname2.split(":")
    p1=self.getPortRef(name1, port1)
    p2=self.getPortRef(name2, port2)
    if p1 and p2:
      name='_'.join([name1, port1, name2, port2])
      return self.connect2(name, p1, p2, service)
    else:
      print("Error in connect: No such ports")
    return None
  
  #
  #
  def connect2(self, name, p1, p2, service=False, silent=True):
    if service:
      con_prof = {'port.port_type':'CorbaPort' }
    else:
      con_prof={'dataport.dataflow_type':'push',
              'dataport.interface_type':'corba_cdr' ,
              'dataport.subscription_type':'flush'}
    try:
      prof_req=ConnectorProfile(name, "", [p1, p2], dict2nvlist(con_prof))
      res, prof=p1.connect(prof_req)
    except:
      res="Error  in connect2"
      if not silent :print(res)
      return None
    if not silent :print("connect2", res)
    return prof

  #
  #
  def disconnect(self, portname1, portname2):
    try:
      con=self.find_connection(portname1, portname2)
      if con is None or not con:
        print("No such connrction:", portname1, portname2)
        return False
       
      con.ports[0].disconnect(con.connector_id)
      print("Sucess to disconnect:", portname1, portname2)
      return True
    except:
      print("Fail to disconnect:", portname1, portname2)
      return False

  #
  #
  def getEC(self, name):
    obj=self.resolveRTObject(name)
    if obj :
      ec=obj.get_owned_contexts()[0]
      return ec
    else:
      return None

  #
  #
  def activate(self, name):
    res=None
    obj=self.resolveRTObject(name)
    if obj :
      ec=obj.get_owned_contexts()[0]
      res=ec.activate_component(obj)
    return res

  #
  #
  def deactivate(self, name):
    res=None
    obj=self.resolveRTObject(name)
    if obj :
      ec=obj.get_owned_contexts()[0]
      res=ec.deactivate_component(obj)
    return res

  #
  #
  def reset(self, name):
    res=None
    obj=self.resolveRTObject(name)
    if obj :
      ec=obj.get_owned_contexts()[0]
      res=ec.reset_component(obj)
    return res

  #
  #
  def get_component_state(self, name):
    stat=None
    obj=self.resolveRTObject(name)
    if obj:
      ec=obj.get_owned_contexts()[0]
      stat=ec.get_component_state(obj)
    return stat

  #
  #
  def terminate(self, name):
    try:
      obj=self.resolveRTObject(name)
    except:
      obj=None
  
    if obj:
      obj.exit()
    return None

  ############
  #
  def createInPort(self, name, type=RTC.TimedString, listener=True):
    print("== Create Inport")
    self._datatype[name]=type
    self._data[name] = instantiateDataType(type)
    self._port[name] = OpenRTM_aist.InPort(name, self._data[name])

    if listener:
      if rtm_version() < 20000:
        self._port[name].addConnectorDataListener(
                            OpenRTM_aist.ConnectorDataListenerType.ON_BUFFER_WRITE,
                            RtcDataListener(name, type, self))
      else:
        self._port[name].addConnectorDataListener(
                            OpenRTM_aist.ConnectorDataListenerType.ON_RECEIVED,
                            RtcDataListener(name, type, self))

    self._port[name].initConsumers()
    self._port[name].initProviders()
    self._port[name].setConnectionLimit(10)
    #self.registerInPort(name, self._port[name])

  #
  # Create the OutPort of RTC
  #
  def createOutPort(self, name, type=RTC.TimedString):
    print("== Create Outport")
    self._datatype[name]=type
    self._data[name] = instantiateDataType(type)
    self._port[name] = OpenRTM_aist.OutPort(name, self._data[name],
                                   OpenRTM_aist.RingBuffer(8))

    self._port[name].configure()
    self._port[name].initConsumers()
    self._port[name].initProviders()
    self._port[name].setConnectionLimit(10)
    #self.registerOutPort(name, self._port[name])

  #
  # Create and Register DataPort of RTC
  #
  def createDataPort(self, name, dtype, inout, listener=False):
    try:
      if name in self._port: return self._port[name]

      module=dtype.split('.')
      if len(module) > 1 and not module[0] in globals():
        exec("import "+module[0], globals())
            
      if inout == 'rtcout':
        self.adaptortype[name] = self.getDataType(dtype)
        self.createOutPort(name, self.adaptortype[name][0])
        self.adaptors[name] = self

      elif inout == 'rtcin':
        self.adaptortype[name] = self.getDataType(dtype)
        self.createInPort(name, self.adaptortype[name][0], listener=listener)
        self.adaptors[name] = self
      else:
        return None

      return self._port[name]
    except:
      return None
  #
  #
  def onData(self, name, data):
    try:
      self._rtcmd.onData(data)
    except:
      print("===>", name, data)
    return

  #
  #  Get DataType
  #
  def getDataType(self, s):
    if len(s) == 0         : return (TimedString, str, False)
    seq = False

    if s[-3:] == "Seq"     : seq = True

    dtype = str
    if sys.version_info.major == 2 and s.count("TimedWString")  : dtype = unicode
    elif s.count("TimedWString"): dtype = str          
    elif s.count("TimedString") : dtype = str
    elif s.count("TimedFloat")  : dtype = float
    elif s.count("TimedDouble") : dtype = float
    elif s.count("TimedShort")  : dtype = int
    elif s.count("TimedLong")   : dtype = int
    elif s.count("TimedOctet")  : dtype = int
    elif s.count("TimedChar")   : dtype = str
    elif s.count("TimedBoolean"): dtype = int
    else                   : dtype = eval("%s" % s)

    return (eval("%s" % s), dtype, seq)

    #
    #
  def newData(self,name):
    return instantiateDataType(self._datatype[name])

    #
    #
  def isNew(self,name):
    try:
      return self._port[name].isNew()
    except:
      return False

  #
  # Send Data 
  #
  def send(self, name, data, code='utf-8', raw=False, tm=False):
    dtype = self.adaptortype[name][1]
    if raw :
      try:
        print(data)
        if type(data) == str :
          ctm=time.time()
          tm="RTC.Time(%d,%d)" % (int(ctm), int((ctm - int(ctm))*1000000000))
          data=data.replace('{time}', tm)
          self._data[name] = eval(data)
          print(self._data[name])
        else:
          self._data[name] = data
      except:
        return

      self.writeData(name)
      return

    if self.adaptortype[name][2]:
      ndata = []
      if type(data) == str :
        for d in data.split(","):
          ndata.append( convertDataType(dtype, d, code) )
        self._data[name].data = ndata
      else:
        self._data[name] = data

    elif dtype == str:
      if self._datatype[name] == TimedString:
        self._data[name].data = encodeStr(data)
      else: 
        self._data[name].data = data

    elif sys.version_info.major == 2 and dtype == unicode:
      self._data[name].data = unicode(data)

    elif (dtype == int  or dtype == float) :
      try:
        self._data[name].data = dtype(data)
      except:
        return
    else:
      try:
        if type(data) == str :
          arg=eval(data)
          self._data[name] = dtype(*arg)
        else:
          self._data[name] = data
      except:
        return
    if tm:
      try:
        ctm=time.time()
        self._data[name].tm.sec = int(ctm) 
        self._data[name].tm.nsec = int((ctm - self._data[name].tm.sec) * 1000000000)
      except:
        pass

    self.writeData(name)

  #
  #
  def writeData(self, name, no_thread=True):
    #print("====>",self._data[name])
    try:
      if no_thread:
        return self._port[name].write(self._data[name])
      else:
        if self._send_thread :
          self._send_thread.join(1)
        self._send_thread=threading.Thread(target=self._port[name].write, name="send_data", args=(self._data[name],))
        self._send_thread.start()
    except:
      pass
  #
  #
  def getData(self,name):
    try:
      return self._data[name]
    except:
      return None

  #
  #
  def readData(self,name):
    try:
      return self._port[name].read()
    except:
      return None

  #
  #
  def readAllData(self,name):
    try:
      res=[]
      while self._port[name].isNew():
        res.append(self._port[name].read())
      return res
    except:
      return []

  def get_connectors_of_rtc(self, name):
    cons = self.getAllConnectors(name)
    cons = sum(cons, [])
    res = []

    for con in cons:
      #print("--", con.ports[0].get_port_profile().name, " <==> ", name, name.split('/')[-1].split('.')[0])
      if con.ports[0].get_port_profile().name.startswith(name.split('/')[-1].split('.')[0]):
        pp=con.ports[0].get_port_profile()
        
        pprof =  nvlist2dict(pp.properties)
        
        if pprof['port.port_type'] == 'DataInPort':
          con.ports.reverse()
        elif pprof['port.port_type'] == 'CorbaPort':
          inp=pp.interfaces[0]
          if inp.polarity == PROVIDED:
            con.ports.reverse()
        res.append([get_obj_port(x) for x in con.ports])
    return res

  #
  # test
  def createGraph(self, rtcs="all", view=True):
    if rtcs == "all" or rtcs == "":
      rtc_names = self.getRTObjectNames()
    else:
      rtc_names=rtcs.split(" ")
    graph = pydot.Dot(graph_type='graph')
    links=[]
    nodes={}
    for name in rtc_names:
      st=self.get_component_state(name.strip())
      color="blue"
      fcolor='white'
      if st == RTC.ACTIVE_STATE:
        color="green"
        fcolor='black'
      elif st == RTC.ERROR_STATE:
        color="red"
      
      disp_name = name.split('/')[-1].split(".")[0]
      nodes[name] = pydot.Node(disp_name, style="filled", fillcolor=color, shape="rect", fontcolor=fcolor)
      graph.add_node(nodes[name])
      cons =self.get_connectors_of_rtc(name)

      if cons : links = links + cons

    for lnk in links:
      oobj, oport = lnk[0]
      iobj, iport = lnk[1]
      lbl=" [%s => %s] " % (oport, iport)
      edge = pydot.Edge(nodes[oobj], nodes[iobj], dir="forward", label=lbl)
      graph.add_edge(edge)

    if view:
      showGraph(graph)
    
    return graph

  def updateGraphImage(self, fname='rtcmd.png'):
    self._loop = True
    while self._loop:
      try:
        graph = self.createGraph('all', False)
        graph.write_png(fname)
      except:
        print("==> Fail to update ", fname)
        pass
      time.sleep(1)

  def startGraphUpdate(self):
    if not self._update_thread :
      self._update_thread=threading.Thread(target=self.updateGraphImage)
      self._update_thread.start()
  
  def stopGraphUpdate(self):
    if self._update_thread :
      self._loop=False
      self._update_thread.join(10)
      self._update_thread=None

  #----- END OF Rtc_Sh

def get_obj_port(port):
  pp=port.get_port_profile()
  pprof =  nvlist2dict(pp.properties)
  if pprof['port.port_type'] == 'CorbaPort':
    inp=pp.interfaces[0]
    if inp.polarity == PROVIDED:
      pname="P:"+pp.name.split(".")[-1]
    else:
      pname="C:"+pp.name.split(".")[-1]
  else:
    pname=pp.name.split(".")[-1]
  comp=nvlist2dict(pp.owner.get_component_profile().properties)['naming.names']
  return [comp, pname]

##################################################################
#   cass RtCmd
#
class RtCmd(cmd.Cmd):
  #intro="Welcome to RtCmd"
  prompt="=> "
  file=None

  #
  #
  def __init__(self, rtsh=None, once=False):
    cmd.Cmd.__init__(self)
    if rtsh is None:
      try:
        self.rtsh=Rtc_Sh()
        self.rtsh.getRTObjectList()

      except:
        self.rtsh=None
        print("Error: NameService not found.")
        #os._exit(-1)
    else:
      self.rtsh=rtsh

    if self.rtsh:  self.rtsh.setRtcmd(self)
    self.onecycle=once
    self.end=False

    self._info=""
    self.processes = []

    self._error=0
    self._rtc_state=None
    self.loop=False
    self.print_conection=None
    self.print_callback=None
    self.print_formatter=None
  
  #
  #
  def __del__(self):
    self.close()
    if self.rtsh :
      del self.rtsh

  #
  #
  def no_rtsh(self):
    if self.rtsh is None:
      print("No NameService")
      self._error = 1
      return True
    return False

  ###
  #  CPMMAND: 
  def do_start_graph(self, arg):
    if self.no_rtsh() : return self.onecycle
    self.rtsh.startGraphUpdate()
    return self.onecycle

  ###
  #  CPMMAND: 
  def do_stop_graph(self, arg):
    if self.no_rtsh() : return self.onecycle
    self.rtsh.stopGraphUpdate()
    return self.onecycle

  ###
  #  CPMMAND: list
  def do_list(self, arg):
    if self.no_rtsh() : return self.onecycle
    num=0
    argv=arg.split()
    l_flag=False

    if len(argv) > 0:
      if argv[0] == '-r':
        self.rtsh.refreshObjectList()
      elif argv[0] == '-l':
        l_flag=True
      else:
        print("Invalid option")
  
    print("===== RTCs =====")
    res = self.rtsh.getRTObjectList()

    for n in res:
      num += 1
      if n[1]:
        stat=self.rtsh.get_component_state(n[0])
        if stat == RTC.ACTIVE_STATE:
          comp_name = n[0]+"*"
        elif stat == RTC.ERROR_STATE:
          comp_name = n[0]+"[X]"
        else:
          comp_name = n[0]
         
        print(num, ":", comp_name)
        if l_flag:
          ports=self.rtsh.getPorts(n[0])
          for pp in ports:
            pname=pp[0].split('.')[1]
            cons=self.rtsh.getConnectors(n[0], pname)
            typ=pp[1]['port.port_type']
            if cons:
              con_str="\n        +- "+str([ c.name for c in cons])
            else:
              con_str=""

            if typ == "DataInPort":
              d_typ=pp[1]['dataport.data_type'].split(":")[1]
              port_str = pname+"("+d_typ+")"
              print("    [in] ->", port_str, con_str)

            elif typ == "DataOutPort":
              d_typ=pp[1]['dataport.data_type'].split(":")[1]
              port_str = pname+"("+d_typ+")"
              print("    [out]<-", port_str, con_str)

            elif typ == "CorbaPort":
              d_typ=pp[1]['interface_type_name']
              if_dir=pp[1]['interface_polarity']
              port_str = pname+"("+d_typ+")"
              if if_dir == PROVIDED:
                print("     [ P ]=o", port_str, con_str)

              else:  # REQUIRED
                print("     [ C ]=C", port_str, con_str)

            else:
              port_str = pname
              print("     --", port_str)

      else:
        print(num, ":[", n[0], "]")
    print("")

    return self.onecycle

  #
  #
  def get_object_names(self, text):
    names=list(self.rtsh.object_list.keys())
    if not text or text == 'all':
      completions=names[:]
    else:
      try:
        completions= [ n for n in names if re.match(text, n) ]
      except:
        pass
    return completions

  #
  #
  def compl_object_name(self, text, line, begind, endidx, sp=""):
    names=list(self.rtsh.object_list.keys())
    if not text:
      completions=names[:]
    else:
      completions= [ n+sp for n in names if n.startswith(text) ]
    return completions 

  #
  #
  def compl_port_name(self, text, line, begind, endidx):
    try:
      objname, pname=text.split(':',1)
      if objname:
        ports=self.rtsh.getPorts(objname)
        pnames=[]
        for pp in ports:
          pnames.append(pp[0].split('.')[1])
        if not pname:
          completions=pnames[:]
        else:
          completions= [ n for n in pnames if n.startswith(pname) ]
      else:
        completions=[]
    except:
      traceback.print_exc()
      completions=[]
      self._error = 1
    return [ objname+":"+p+" " for p in completions]

  ###
  #  COMMAND: get_ports
  def do_get_ports(self, arg):
    if self.no_rtsh() : return self.onecycle
  
    num=0
    ports = self.rtsh.getPorts(arg)
    print("====== Ports(%s) ======" % arg)
    for pp in ports:
      num += 1
      print(num, ":", pp[0].split('.')[1])
      for k in pp[1]:
         print("   ", k,":", pp[1][k])
            
    print("")
    return self.onecycle

  #
  #
  def complete_get_ports(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  #  COMMAND: get_connectors
  def do_get_connectors(self, arg):
    if self.no_rtsh() : return self.onecycle

    try:
      name, port = arg.split(":")
      cons=self.rtsh.getConnectors(name, port)
      if cons is None:
        print("   No connectors")
      else:
        for con in cons:
          info=self.rtsh.getConnectionInfo(con)
          print("   ", info['name'],":", info['ports'][0],"==",info['ports'][1])
    except:
      print("Error in get_connectors:", arg)

  #
  #
  def complete_get_connectors(self, text, line, begind, endidx):
    args=line.split()
    if line[endidx-1] != ' ' and args[-1].find(':') > 0 :
      text=args[-1]
      return self.compl_port_name(text, line, begind, endidx)
    else:
      return self.compl_object_name(text, line, begind, endidx)

  ###
  #  COMMAND: get_connection
  def do_get_connection(self, arg):
    if self.no_rtsh() : return self.onecycle

    argv=arg.split()
    if len(argv) > 1:
      cons = self.rtsh.find_connections(argv[0], argv[1])
      num=1
      if cons:
        for x in cons:
          print(num, ":", format_connection(cons))
          num += 1
      else:
        print("No connection")
        self._error = 1
    else:
      print("get_connection comp1:p comp2:p")
      self._error = 1
    return self.onecycle

  #
  #
  def complete_get_connection(self, text, line, begind, endidx):
    args=line.split()
    if line[endidx-1] != ' ' and args[-1].find(':') > 0 :
      text=args[-1]
      return self.compl_port_name(text, line, begind, endidx)
    else:
      return self.compl_object_name(text, line, begind, endidx, ":")

  ###
  #  COMMAND: connect
  def do_connect(self, arg):
    if self.no_rtsh() : return self.onecycle

    argv=arg.split()
    if len(argv) > 1:
      res=self.rtsh.connect(argv[0], argv[1])
      if res is None: self._error = 1
    else:
      print("connect comp1:p comp2:p")
      self._error=1
    return self.onecycle

  #
  #
  def complete_connect(self, text, line, begind, endidx):
    args=line.split()
    try:
      self._info=args[1]
    except:
      self._info=""

    if line[endidx-1] != ' ' and args[-1].find(':') > 0 :
      text=args[-1]
      if(len(args) > 2):
        return self.compl_inport_name(text, line, begind, endidx, self._info)
      else:
        return self.compl_outport_name(text, line, begind, endidx)
    else:
      return self.compl_object_name(text, line, begind, endidx, ":")

  #
  #
  def compl_inport_name(self, text, line, begind, endidx, info=""):
    try:
      objname, pname=text.split(':',1)
      if objname:
        if info:
          oname = info.split(":")
          dtype = self.rtsh.getPortDataType(oname[0], oname[1])
        else:
          dtype = info

        ports=self.rtsh.getPorts(objname, isDataInport, dtype)
        pnames=[]
        for pp in ports:
          pnames.append(pp[0].split('.')[1])
        if not pname:
          completions=pnames[:]
        else:
          completions= [ n for n in pnames if n.startswith(pname) ]
      else:
        completions=[]
    except:
      traceback.print_exc()
      completions=[]
    return [ objname+":"+p for p in completions]

  #
  #
  def compl_outport_name(self, text, line, begind, endidx, sp=" "):
    try:
      objname, pname=text.split(':',1)
      if objname:
        ports=self.rtsh.getPorts(objname, isDataOutport)
        pnames=[]
        for pp in ports:
          pnames.append(pp[0].split('.')[1])
        if not pname:
          completions=pnames[:]
        else:
          completions = [ n for n in pnames if n.startswith(pname) ]
      else:
        completions=[]
    except:
      traceback.print_exc()
      completions=[]
    if len(completions) == 1:
      return [ objname+":"+p+sp for p in completions]
    else:
      return [ objname+":"+p for p in completions]

 ###
  #  COMMAND: connect_service
  def do_connect_service(self, arg):
    if self.no_rtsh() : return self.onecycle

    argv=arg.split()
    if len(argv) > 1:
      res=self.rtsh.connect(argv[0], argv[1], True)
      if res is None: self._error = 1
      else: print("OK")
    else:
      print("connect comp1:p comp2:p")
      self._error=1
    return self.onecycle

  #
  #
  def compl_provider_name(self, text, line, begind, endidx):
    try:
      objname, pname=text.split(':',1)
      if objname:
        ports=self.rtsh.getPorts(objname, isServicePort)
        pnames=[]
        for pp in ports:
          if 'interface_polarity' in pp[1] and pp[1]['interface_polarity'] == PROVIDED:
            pnames.append(pp[0].split('.')[1])
        if not pname:
          completions=pnames[:]
        else:
          completions= [ n for n in pnames if n.startswith(pname) ]
      else:
        completions=[]
    except:
      traceback.print_exc()
      completions=[]
    return [ objname+":"+p for p in completions]

  #
  #
  def compl_consumer_name(self, text, line, begind, endidx, sp=" "):
    try:
      objname, pname=text.split(':',1)
      if objname:
        ports=self.rtsh.getPorts(objname, isServicePort)
        pnames=[]
        for pp in ports:
          if 'interface_polarity' in pp[1] and pp[1]['interface_polarity'] == REQUIRED:
            pnames.append(pp[0].split('.')[1])
        if not pname:
          completions=pnames[:]
        else:
          completions = [ n for n in pnames if n.startswith(pname) ]
      else:
        completions=[]
    except:
      traceback.print_exc()
      completions=[]
    if len(completions) == 1:
      return [ objname+":"+p+sp for p in completions]
    else:
      return [ objname+":"+p for p in completions]

  def complete_connect_service(self, text, line, begind, endidx):
    args=line.split()
    try:
      self._info=args[1]
    except:
      self._info=""

    if line[endidx-1] != ' ' and args[-1].find(':') > 0 :
      text=args[-1]
      if(len(args) > 2):
        return self.compl_provider_name(text, line, begind, endidx)
      else:
        return self.compl_consumer_name(text, line, begind, endidx)
    else:
      return self.compl_object_name(text, line, begind, endidx, ":")
  ###
  #  COMMAND: disconnect
  def do_disconnect(self, arg):
    if self.no_rtsh() : return self.onecycle

    argv=arg.split()
    if len(argv) > 1:
      res=self.rtsh.disconnect(argv[0], argv[1])
      if not res: self._error = 1 
    else:
      print("disconnect comp1:p comp2:p")
      self._error = 1
    return self.onecycle

  #
  #
  def complete_disconnect(self, text, line, begind, endidx):
    return self.complete_connect(text, line, begind, endidx)

  ###
  #  COMMAND: activate
  def do_activate(self, arg):
    if self.no_rtsh() : return self.onecycle

    argv=arg.split()
    self.rtsh.refreshObjectList()

    for v in argv:
      objs = self.get_object_names(v)
      for obj in objs:
        res=self.rtsh.activate(obj)
        #if res == RTC.RTC_ERROR: self._error=1
    return self.onecycle

  #
  #
  def complete_activate(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  #  COMMAND: deactivate
  def do_deactivate(self, arg):
    if self.no_rtsh() : return self.onecycle

    self.rtsh.refreshObjectList()

    argv=arg.split()
    for v in argv:
      objs = self.get_object_names(v)
      for obj in objs:
        res=self.rtsh.deactivate(obj)
        #if res == RTC.RTC_ERROR: self._error=1
    return self.onecycle

  #
  #
  def complete_deactivate(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  #  COMMAND: get_state
  def do_get_state(self, arg):
    if self.no_rtsh() : return self.onecycle

    stat=self.rtsh.get_component_state(arg)
    print("State:", arg,":", stat)
    self._rtc_state=stat
    return self.onecycle

  #
  #
  def complete_get_state(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  #  COMMAND: reset
  def do_reset(self, arg):
    if self.no_rtsh() : return self.onecycle

    self.rtsh.refreshObjectList()

    argv=arg.split()
    for v in argv:
      objs = self.get_object_names(v)
      for obj in objs:
        res=self.rtsh.reset(obj)
        if res == RTC.RTC_ERROR: self._error=1
    return self.onecycle
  #
  #
  def complete_reset(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  #  COMMAND: terminate
  def do_terminate(self, arg):
    if self.no_rtsh() : return self.onecycle

    self.rtsh.refreshObjectList()
    argv=arg.split()
    for v in argv:
      objs = self.get_object_names(v)
      for obj in objs:
        res=self.rtsh.terminate(obj)
        if res == RTC.RTC_ERROR: self._error=1
    return self.onecycle

  #
  #
  def complete_terminate(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  #  COMMAND: unbind
  def do_unbind(self, arg):
    if self.no_rtsh() : return self.onecycle

    argv=arg.split()
    for v in argv:
      res = self.rtsh.unbind(v)
      if not res: self._error = 1

    return self.onecycle

  def complete_unbind(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  #  COMMAND: system
  def do_system(self, arg):
    cmdline = arg.split()
    try:
      proc = subprocess.Popen(cmdline, shell=True)
      self.processes.append(proc)
    except:
      traceback.print_exc()
      self._error = 1

  ###
  #  COMMAND: start
  def do_start(self, arg):
    arg = "cmd /c start "+arg
    cmdline = arg.split()
    try:
      proc = subprocess.Popen(cmdline)
      self.processes.append(proc)
    except:
      traceback.print_exc()
      self._error = 1

  ###
  #  COMMAND: killall
  def do_killall(self, arg):
    print(self.processes)
    for p in self.processes:
      if p.poll() is None:
        print("--wait--", p)
        p.terminate()
        p.wait(5)

  ###
  #  COMMAND: refresh
  def do_refresh(self,arg):
    if self.no_rtsh() : return self.onecycle
    
    self.rtsh.refreshObjectList()
 
    return self.onecycle
  ###
  #  COMMAND: launch
  def do_launch(self, arg):
    argv = arg.split(" ")
    verbose=False
    fname=None
    for i in range(len(argv)):
      if argv[i] == "-v":
        verbose=True
      elif fname is None:
        fname = argv[i]
      else:
        pass
  
    if fname is None or not os.path.exists(fname):
      self._error=1
      print("No such file:", fname)
      return self.onecycle

    with open(fname, "r", encoding="utf-8") as f:
      cmds = f.read()
      for cmdline in cmds.split("\n"):
        cmd = cmdline.split("#")[0].strip()
        if cmd :
          if cmd.startswith('print'): cmd = cmdline
          if verbose: print("execute:", cmd)
          self.onecmd(cmd)
          if self._error > 0:
            print("ERROR", self._error)
            return self.onecycle
    self.onecycle

  ###
  #  COMMAND: sleep
  def do_sleep(self, arg):
    time.sleep(int(arg))
    self.onecycle

  ###
  #  COMMAND: bye
  def do_bye(self, arg):
    print('...BYE')
    for p in self.processes:
      if p.poll() is None:
        p.terminate()
        p.wait()
    self.close()
    self.end=True
    return True

  # ----- record and playback 
  #
  #  COMMAND: record
  def do_record(self, arg):
    self.file = open(arg, "w", encoding="utf-8")
    self.onecycle

  ###
  #  COMMAND: playback
  def do_playback(self, arg):
    self.close()
    with open(arg, "r", encoding="utf-8") as f:
      self.cmdqueue.extend(f.read().splitlines())
    self.onecycle
  #
  #
  def precmd(self, line):
    #line = line.lower()
    self._error = 0
    self._rtc_state=None

    if self.file and 'playback' not in line:
      print(line, file=self.file)
    return line

  #  record and playback -----

  #
  #
  def close(self):
    self.loop=False
    self.rtsh.disconnectAllPorts()
    #if self.print_conection:
    #  self.print_conection[0].disconnect(self.print_conection[1])
    self.rtsh.stopGraphUpdate()

    if self.file:
      self.file.close()
      self.file = None

    if self.rtsh and self.rtsh.manager:
      self.rtsh.manager.shutdown()

  #
  #
  def emptyline(self):
    return

  #
  #
  def completenames(self, text, *ignored):
    dotext = 'do_'+text
    retval = [a[3:]+" " for a in self.get_names() if a.startswith(dotext)]
    return retval


  ###
  #  COMMAND: inject
  def do_inject(self, args):
    if self.no_rtsh() : return self.onecycle
  
    argv=args.split(" ")
    cname=None
    pname=None
    formatter=None
    nloop=0
    data=""
    intval = 1
    timeout=0
    raw=True

    while argv:
      arg = argv.pop(0)
      if arg.startswith("#") : break
      if arg == "-m":
        try:
          module="import "+ argv.pop(0)
          exec(module, globals())
        except:
          print("Error in import module")
          pass

      elif arg == "-n":
        try:
          nloop=int(argv.pop(0))
        except:
          print("Invalid options")
          pass
      elif arg == "-r":
        try:
          intval=1.0/float(argv.pop(0))
        except:
          print("Invalid options")
          pass
      elif arg == "-t":
        try:
          timeout=float(argv.pop(0))
        except:
          print("Invalid options")
          pass
      elif arg == "--seat":
        raw=False
      elif arg == "-p":
        try:
          arg = argv.pop(0)
          if not (arg in sys.path):
            sys.path.append(arg)
        except:
          print("Invalid options")
          pass
      elif arg == "-c":
        data = ' '.join(argv)
        break

      else:
        if arg.find(":") > 0 and cname is None:
          cname, pname = arg.split(":")
          try:
            pname, formatter = pname.split("#")
          except:
            pass

    dtype = self.rtsh.getPortDataType(cname, pname)
    if dtype :
      dtype2 = dtype.split(":")[1].replace("/", ".")
      pref = self.rtsh.getPortRef(cname, pname)
      #
      # Create inport and connect
      if self.rtsh.manager is None:
        self.rtsh.initRtmManager()
        self.rtsh.refreshObjectList()

      portname = "inject_"+dtype2
      port=self.rtsh.createDataPort(portname, dtype2, "rtcout")
      if port is None:
        print("Fail to create DataPort")
        self._error = 1
        return self.onecycle
      cprof=self.rtsh.connect2(portname+"_"+cname+"_"+pname, port._objref, pref)
      print("=== inject ====")

      #
      # send data
      ctm=time.time()
      if not data.strip() :
        self.loop = True
        count=0
        while self.loop:
          if nloop > 0 and count >= nloop: break
          print("inject ==> ", end="")
          try:
            data=input()
            self.sendData(portname, data, raw)
            count += 1
          except EOFError:
            self.loop = False
            print("")
      else:
        if timeout > 0:
          while True:
            self.sendData(portname, data, raw)
            time.sleep(intval)
            if time.time() > ctm+timeout: break
        else:
          if nloop <= 0: nloop=1
          for i in range(nloop):
            self.sendData(portname, data, raw)
            if i < nloop-1:
              time.sleep(intval)
      
      #
      # disconnect
      port.disconnect(cprof.connector_id)
      #print("-- disconnect inject",self.onecycle)

    return self.onecycle

  #
  #
  def complete_inject(self, text, line, begind, endidx):
    args=line.split()

    if line[endidx-1] != ' ' and args[-1].find(':') > 0 :
      text=args[-1]
      return self.compl_inport_name(text, line, begind, endidx)
    else:
      return self.compl_object_name(text, line, begind, endidx, ":")

  def sendData(self, portname, data, raw=False, tm=True):
    self.rtsh.send(portname, data, raw=raw, tm=tm)

  ###
  # COMMAND: print
  def do_print(self, args):
    if self.no_rtsh() : return self.onecycle
    argv=args.split(" ")
    cname=None
    pname=None
    formatter=None
    nloop=1
    timeout=0
    intval=1
    listener=False
    callback=None

    while argv:
      arg = argv.pop(0)
      if arg.startswith("#") : break
      if arg == "-m":
        try:
          module="import "+ argv.pop(0)
          exec(module, globals())
        except:
          print("Error in import module")
          pass
      elif arg == "-n":
        try:
          nloop=int(argv.pop(0))
        except:
          print("Invalid options")
          pass
      elif arg == "-r":
        try:
          intval=1.0/float(argv.pop(0))
        except:
          print("Invalid options")
          pass
      elif arg == "-t":
        try:
          timeout=float(argv.pop(0))
        except:
          print("Invalid options")
          pass
      elif arg == "-p":
        try:
          arg = argv.pop(0)
          if not (arg in sys.path):
            sys.path.append(arg)
        except:
          print("Invalid options")
          pass
      elif arg == "-l":
        if self.print_conection:
          return self.onecycle
        else:
          listener=True

      elif arg == "-c":
        try:
          callback=argv.pop(0)
        except:
          print("Invalid options")
          pass
      else:
        if arg.find(":") > 0 and cname is None:
          cname, pname = arg.split(":")
          try:
            pname, formatter = pname.split("#")
          except:
            try:
              pname, formatter = pname.split("$")
            except:
              pass

    dtype = self.rtsh.getPortDataType(cname, pname)

    if dtype :
      dtype2 = dtype.split(":")[1].replace("/", ".")
      pref = self.rtsh.getPortRef(cname, pname)
      #
      # Create inport and connect
      if self.rtsh.manager is None:
        self.rtsh.initRtmManager()
        self.rtsh.refreshObjectList()

      portname = "print_" + dtype2
      port = self.rtsh.createDataPort(portname, dtype2, "rtcin", listener)
      if port is None:
        print("Fail to create DataPort")
        self._error = 1
        return self.onecycle
      cprof=self.rtsh.connect2(portname+"_"+cname+"_"+pname, port._objref, pref)
      print("=======print:", formatter)
      self.print_formatter=formatter
      if formatter :
        module=formatter.split('.')
        if len(module) > 1 and not module[0] in globals():
          exec("import "+module[0], globals())
      
      try:
        if callback: self.print_callback = eval(callback)
      except:
        pass
      ##############################
      #
      if listener:
        print("Connector_id:", cprof.connector_id)
        self.print_conection=[port, cprof.connector_id]
        pass
      else:
        ctm=time.time()
        if timeout > 0:
          while True:
            if self.rtsh.isNew(portname):
              data = self.rtsh.readData(portname)
            else:
              data=None
            if time.time() > ctm+timeout: break
            # output with formatter
            if data is None:
              print("== No Data")
            else:
              if formatter :
                try:
                  fmt=eval(formatter)
                  print(fmt(data))
                except:
                  print(data)
              else:
                print(data)
            time.sleep(intval)

        else:
          for i in range(nloop):
            #
            # recieve data
            self.loop = True
            while self.loop:
              if self.rtsh.isNew(portname):
                self.loop = False
              time.sleep(0.3)
            data = self.rtsh.readData(portname)

            # output with formatter
            if formatter :
              try:
                fmt=eval(formatter)
                print(fmt(data))
              except:
                print(data)
            else:
              print(data)
        ###########################
        #
        # disconnect
        port.disconnect(cprof.connector_id)
        #print("-- disconnect print",self.onecycle)
      
    #if self.onecycle: self.close()
    return self.onecycle

  #
  #
  def complete_print(self, text, line, begind, endidx):
    args=line.split()

    if line[endidx-1] != ' ' and args[-1].find(':') > 0 :
      text=args[-1]
      return self.compl_outport_name(text, line, begind, endidx, "")
    else:
      return self.compl_object_name(text, line, begind, endidx, ":")


  ###
  #  COMMAND: print_exit
  def do_print_exit(self, arg):
    if self.no_rtsh() : return self.onecycle
    if self.print_conection:
      self.print_conection[0].disconnect(self.print_conection[1])
      self.print_conection=None
    return self.onecycle

  #
  #
  def onData(self, data):
    if self.print_callback:
      self.print_callback(data)
    else:
      if self.print_formatter:
        try:
          fmt=eval(self.print_formatter)
          print(fmt(data))
        except:
          print("--format error: ", end="")
          print(data)
      else:
        print(data)

  ###
  #  COMMAND: disconenct_all
  def do_disconnect_all(self, arg):
    argv = arg.split(" ")
    for name in argv:
      self.rtsh.disconnectAll(name)
    
    return self.onecycle

  def complete_disconnect_all(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  #
  # COMMAD waite_for
  def do_wait_for(self, args):
    try:
      if self.no_rtsh() : return self.onecycle
      func=self.find_rtc
      flag=True
      cname=None

      argv=args.split(" ")
      timeout = float(argv.pop(0))

      while argv:
        arg = argv.pop(0)
        if arg.startswith("#") : break
        if arg == "-m":
          try:
            module="import "+ argv.pop(0)
            exec(module, globals())
          except:
            print("Error in import module")
            pass
        elif arg == "-r":
          flag=False
        elif arg == "-p":
          try:
            arg = argv.pop(0)
            if not (arg in sys.path):
              sys.path.append(arg)
          except:
            print("Invalid options")
            pass
        elif arg == "-f":
          try:
            func=eval(argv.pop(0))
          except:
            print("Invalid options")
            pass
        elif arg == "-c":
          try:
            cname = argv.pop(0)
          except:
            print("Invalid options")
            pass
        else:
          pass

      if cname is None:
        time.sleep(timeout)
        return self.onecycle

      res=self.rtsh.wait_for(cname, timeout, func=func, flag=flag)
      if not res:
        print("==== time out =====")
      else:
        print("OK")
    except:
      traceback.print_exc()
      print("===ERROR===")
    return self.onecycle

  def complete_wait_for(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")

  def find_rtc(self, name):
    if name.count(".rtc") == 0 : name = name+".rtc"
    stat=self.rtsh.refreshObjectList()
    names=list(self.rtsh.object_list.keys())
    for n in names:
      if n.find(name) >= 0: return True
    return False

  def check_active(self, name):
    stat=self.rtsh.get_component_state(name)
    if stat == RTC.ACTIVE_STATE: return True
    else: return False

  ###
  # COMMAND: conf
  def do_conf(self, args):
    if self.no_rtsh() : return self.onecycle
    argv=args.split(" ")

    comp_name=argv.pop(0)
    conf_name=None
    cmd=None
    opt_l=False
    opt_a=False
    set_name=None

    while argv:
      arg = argv.pop(0)
      if arg.startswith("#") : break
      if arg == "-l":
        opt_l=True
      elif arg == "-a":
        opt_a=True
      elif arg == "-la" or arg == "-al":
        opt_a=True
        opt_l=True

      elif arg == "-s":
        try:
          set_name=argv.pop(0)
        except:
          print("Invalid options")
          pass
      else:
        if cmd is None:
          cmd = arg
          cmd_args = argv
          break

    obj=self.rtsh.resolveRTObject(comp_name)
    config = obj.get_configuration()
    try:
      active_conf_set = config.get_active_configuration_set()
    except:
      active_conf_set=None
    if cmd == "list":
      print("=========")
      conf_sets = config.get_configuration_sets()
      conf_set_names = [ c.id for c in conf_sets ]
      conf_set_names.sort()
      if opt_l :
        for n in conf_set_names:
          if (not n.startswith("__")) or opt_a:
            cf = config.get_configuration_set(n)
            if active_conf_set and  n == active_conf_set.id:
              print_configuration_set(cf, "*")
            else:
              print_configuration_set(cf)
      else:
        for n in conf_set_names:
          if (not n.startswith("__")) or opt_a:
            if active_conf_set and n == active_conf_set.id :
              print("+", n, "*")
            else:
              print("+", n)

      print("")

    elif cmd == "set":
      if set_name is None: set_name=active_conf_set.id
      try:
        conf = config.get_configuration_set(set_name)
        for data  in conf.configuration_data:
          if argv[0] == data.name:
            data.value._v = argv[1]
            config.set_configuration_set_values(conf)
            if conf.id == active_conf_set.id:
              config.activate_configuration_set(conf.id)
            break
      except:
        print("==ERROR==", argv)
        traceback.print_exc()

    elif cmd == "get":
      if set_name is None: set_name=active_conf_set.id
      try:
        for data  in config.get_configuration_set(set_name).configuration_data:
          if argv[0] == data.name: 
            print(data.value.value())
      except:
        print("==ERROR==", argv)

    elif cmd == "act":
      try:
        config.activate_configuration_set(argv[0])
      except:
        print("==Error==")
    else:
      print("Invalid command", cmd)

    return self.onecycle

  #
  #
  def complete_conf(self, text, line, begind, endidx):
    args=line.split()
    if len(args) > 1 :
      pass
    
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  # COMMAND: doc
  def do_doc(self, args):
    if self.no_rtsh() : return self.onecycle
    argv=args.split(" ")

    profile = self.rtsh.getRtComponentInfo(argv[0])
    rtc_doc(profile)

    return self.onecycle

  def complete_doc(self, text, line, begind, endidx):
    args=line.split()
    if len(args) > 1 :
      pass
    
    return self.compl_object_name(text, line, begind, endidx, " ")

  ###
  # COMMAND: graph
  def do_graph(self, args):
    if self.no_rtsh() : return self.onecycle

    try:
      self.rtsh.createGraph(args)
    except:
      print("===No PIL found. please install PIL, and pydot")
      traceback.print_exc()

    return  self.onecycle

  def complete_graph(self, text, line, begind, endidx):
    return self.compl_object_name(text, line, begind, endidx, " ")
#------ END OF RtCom


############# for Documentation #####################
import yaml
import html
import webbrowser


def rtc_doc(fname, arg_type=None):
  profile=None
  if arg_type == "file":
    with open(fname, encoding="utf-8") as file:
      profile = yaml.load(file)
  else:
    profile=fname

  if not profile :
    print("=== No Profile")
    return None


  res='''
  <html>
   <head>
     <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
     <meta name="viewport" content="width=device-width,initial-scale=1.0" >
     <style>
      table, th, td {
       border-collapse: collapse;
       border: 1px solid #000;
       line-height: 1.5;
      }
      table th{
        background: #d9d9d9;
      }
     </style>
   </head>
  <body>
  '''

  res +="<h1>"+profile['name']+"</h1>\n"
  res +="<h2>Profile</h2>\n"
  res+="<table><tr><td>\n"
  res += rtc_profile(profile)
  res+="</td><td>\n"
  res += rtc_image(profile)
  res+="</td></tr></table>\n"
  dp=rtc_dataport_table(profile)
  if dp:
    res +="<h2>Data Ports</h2>\n"
    res += dp

  sp = rtc_serviceport_table(profile)
  if sp :
    res +="<h2>Service Ports</h2>\n"
    res += sp
  cf=rtc_configuration_table(profile)
  if cf:
    res +="<h2>Configurations</h2>\n"
    res +=cf 
  res +="</body></html>"

  docname=profile['name']+".html"

  with open(docname, "w", encoding="utf-8") as file:
    file.write(res)

  webbrowser.open(docname)

#
#
#
def rtc_image(profile, view_w=600, pos0=260, align='center', style='', style2=''):
  res = ""
  if profile :
    pos=pos0
    n=0
    n_l=0
    n_r=0
    port_r=[]
    port_l=[]

    #  count data_ports
    if 'dataport' in profile:
      dataport = profile['dataport']
      if dataport:
        for port in dataport:
          if port['flow'] == 'in' or port['flow'] == 'DataInPort':
            n_l += 1
            port_l.append(port)
            if n < n_l : n = n_l
          elif port['flow'] == 'out' or port['flow'] == 'DataOutPort':
            n_r += 1
            port_r.append(port)
            if n < n_r: n = n_r

    # count service_ports
    if 'serviceport' in profile and  profile['serviceport'] :
      serviceport = profile['serviceport']
      if serviceport:
        for port in serviceport :
          if port['flow'] == 'provider' :
            if 'place' in port and port['place'] == 'left':
              n_l += 1
              port_l.append(port)
              if n < n_l: n = n_l
            else:
              n_r += 1
              port_r.append(port)
              if n < n_r: n = n_r
  
          elif port['flow'] == 'consumer' :
            if 'place' in port and port['place'] == 'left':
              n_l += 1
              port_l.append(port)
              if n < n_l: n = n_l
            else:
              n_r += 1
              port_r.append(port)
              if n < n_r: n = n_r

    # Draw image
    port_h = 50
    if n == 0:
      height=50
    else:
      height= 50 + port_h*(n -1)
    width=80

    view_h= height + 40

    rtc_name = profile['name']
    
    text_pos_x = pos + width/2
    text_pos_y = height + 30

    res='''
<svg width="$view_w" viewBox="0 0 $view_w $view_h" style="$style" >
     <defs>
        <path id="in_l" d="M0,0 h20 v20 h-20 L10,10Z" stroke="black" stroke-width="1" fill="green"/>
        <path id="out_r" d="M0,0 h10 L20,10 L10,20 h-10 Z" stroke="black" stroke-width="1" fill="green"/>

        <g id="consumer_r" stroke="black" stroke-width="1">
          <path d="M 0,0 h16 v16 h-16 Z" fill="green" />
          <path d="M 16,8 h8" />
          <path d="M 31,0 a 8,8 0 0,0 0,16" fill="none"/>
        </g>
        <g id="provider_r" stroke="black" stroke-width="1" fill="green">
          <path d="M 0,0 h16 v16 h-16 Z" />
          <path d="M 16,8 h8" />
          <circle cx="30" cy="8" r="6" />
        </g>
        <g id="consumer_l" stroke="black" stroke-width="1">
          <path d="M 0,0 h16 v16 h-16 Z" fill="green" />
          <path d="M 0,8 h -8" />
          <path d="M -16,0 a 8,8 0 0,1 0,16" fill="none" />
        </g>
        <g id="provider_l" stroke="black" stroke-width="1" fill="green">
          <path d="M 0,0 h16 v16 h-16 Z" />
          <path d="M 0,8 h-8" />
          <circle cx="-14" cy="8" r="6"/>
        </g>
    </defs>
    <rect x="$pos" y="10" width="$width" height="$height" fill="blue" stroke="black" />
    <text x="$text_pos_x" y="$text_pos_y" text-anchor="middle">$rtc_name</text>
    '''
    res=res.replace('$align', align)
    res=res.replace('$style2', style2)
    res=res.replace('$style', style)
    res=res.replace('$view_w', str(view_w))
    res=res.replace('$view_h', str(view_h))
    res=res.replace('$pos', str(pos))
    res=res.replace('$width', str(width))
    res=res.replace('$height', str(height))
    res=res.replace('$text_pos_x', str(text_pos_x))
    res=res.replace('$text_pos_y', str(text_pos_y))
    res=res.replace('$rtc_name', rtc_name)
    # Left side
    i=0
    for port in port_l:
      posx = pos - 15
      posy = 25 + port_h * i

      portname = port['name']
      if port['flow'] == 'DataInPort':
        portflow="in"
      elif port['flow'] == 'DataOutPort':
        portflow="out"
      else:
        portflow = port['flow']

      posy1 = posy+5
      posy2 = posy+20

      if portflow == 'in':
        posx1 = posx-10
        porttype = port['type']

      elif portflow == 'consumer' or portflow == 'provider':
        posx1 = posx-25
        porttype = port['if_type_name']
        if not porttype :
          porttype = port['interface']

      res +='''
    <use xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="#${portflow}_l"  x="$posx" y="$posy" />
    <text x="$posx1" y="$posy1" text-anchor="end">$portname</text>
    <text x="$posx1" y="$posy2" text-anchor="end">($porttype)</text>
      '''
      res=res.replace("${portflow}", portflow)
      res=res.replace("$posx1", str(posx1))
      res=res.replace("$posy1", str(posy1))
      res=res.replace("$posy2", str(posy2))
      res=res.replace("$posx", str(posx))
      res=res.replace("$posy", str(posy))
      res=res.replace("$portname", portname)
      res=res.replace("$porttype", porttype)

      i += 1

    # Right side
    i=0
    for port in port_r :
      posx = pos + width -5
      posy = 25 + port_h * i

      portname = port['name']
      if port['flow'] == 'DataInPort':
        portflow="in"
      elif port['flow'] == 'DataOutPort':
        portflow="out"
      else:
        portflow = port['flow']

      posy1 = posy+5
      posy2 = posy+20

      if portflow == 'out':
        posx1 = posx+30
        porttype = port['type']

      elif portflow == 'consumer' or portflow == 'provider':
        porttype = port['if_type_name']
        if not porttype:
          porttype = port['interface']

        posx1 = posx+40

      res +='''
    <use xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="#${portflow}_r"
      x="$posx" y="$posy" />
    <text x="$posx1" y="$posy1" text-anchor="start">$portname</text>
    <text x="$posx1" y="$posy2" text-anchor="start">($porttype)</text>
      '''

      res=res.replace("${portflow}", portflow)
      res=res.replace("$posx1", str(posx1))
      res=res.replace("$posy1", str(posy1))
      res=res.replace("$posy2", str(posy2))
      res=res.replace("$posx", str(posx))
      res=res.replace("$posy", str(posy))
      res=res.replace("$portname", portname)
      res=res.replace("$porttype", porttype)

      i += 1

    res += "</svg>\n"

  return res


def rtc_profile(profile):
  res=""

  res += "<table>"
  keys=['instance_name', 'description', 'version', 'vendor', 'category', 'component_type', 'activity_type', 'kind', 'max_instance', 
    'language', 'lang_type', 'exec_cxt.periodic.type','exec_cxt.periodic.rate',
    'openrtm.name', 'openrtm.version', 'os.name', 'corba.id']

  for k in keys:
    if k in profile:
      if  type(profile[k]) == str: 
        res += "<tr><th>%s</th><td>%s</td></tr>\n" % (k, profile[k].encode('raw-unicode-escape').decode('utf-8', 'ignore'))
      else:
         res += "<tr><th>%s</th><td>%s</td></tr>\n" % (k, profile[k])

  res += "</table>"
  return res
#
#
#
def rtc_dataport_table(profile, klass="docutils", colw="18:15:15:52"):
  res=""

  try:
    dataport = profile['dataport']

    if not dataport:
      res =""
      return res

    res ='''
      <table width="100%" class="$class">
        <colgroup>
    '''
    res = res.replace("$class", klass)

    cols = colw.split(":")
    for val in cols :
      res += "<col width=\""+val+"%\">\n"

    res +='''
        </colgroup>
        <thead><th>Name</th><th>Flow</th><th>Data Type</th><th>Description</th></thead>
        <tbody>
    '''

    for port in dataport :
      name=""
      flow=""
      typ=""
      description=""
      if 'name' in port: name = port['name']
      if name:
        if 'type' in port: typ = port['type']
        if 'description' in port: description = port['description'].encode('raw-unicode-escape').decode('utf-8', 'ignore')
        if 'flow' in port and (port['flow'] == 'in' or port['flow'] =='DataInPort'):
          flow = "InPort"
        elif 'flow' in port and (port['flow'] == 'out' or port['flow'] == 'DataOutPort') :
          flow = "OutPort"
        else:
          flow = ""
      res +="<tr><td>$name </td><td>$flow</td><td>$type</td><td>$description</td></tr>\n"
      res=res.replace("$name", name)
      res=res.replace("$flow", flow)
      res=res.replace("$type", typ)
      res=res.replace("$description", description)

    res +="\n</tbody>\n</table>"

  except:
    pass

  return res


def rtc_serviceport_table(profile, klass="docutils", colw="18:15:15:52"):
  res=""

  try:
    serviceport = profile['serviceport']

    if not serviceport:
      res =""
      return res

    res='''
      <table width="100%" class="$class">
        <colgroup>
    '''
    res = res.replace("$class", klass)
    cols = colw.split(":")
    for val in cols :
      res += "<col width=\"" +val +"%\">\n"

    res +='''
        </colgroup>
        <thead><th>Name</th><th>Service Type</th><th>Interface</th><th>Description</th></thead>
        <tbody>
    '''

    for port in serviceport :
      name=""
      flow=""
      typ=""
      description=""
      if 'name' in port : name = port['name']
      if name:
        if 'if_type_name' in port: typ = port['if_type_name']
        if not typ:
          if 'interface' in port : typ = port['interface']

        if 'description' in port: description = port['description']

        if 'flow' in port and port['flow'] == 'provider':
          flow = "Provider";
        elif 'flow' in port and port['flow'] == 'consumer':
          flow = "Consumer"
        else:
          pass
        res +="<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n" % (name, flow, typ, description)
    res +="</tbody>\n  </table>"
  except:
    pass
  return res

#
#
#
def rtc_configuration_table(profile, klass="docutils", colw="15:10:15:15:45"):
  res=""

  try:
    configuration = profile['configuration']
    if not configuration:
      res =""
      return res

    res='''
      <table width="100%" class="$class">
        <colgroup>
    '''
    res=res.replace("$class", klass)

    cols = colw.split(":")
    for val in cols :
      res += "<col width=\""+val+"%\">\n"

    res +='''
        </colgroup>
        <thead><th>Name</th><th>Data Type</th><th>Deafult Value</th><th>Constraints</th><th>Description</th></thead>
        <tbody>
    '''

    for port in configuration:
      name=""
      constraints=""
      typ=""
      description=""
      defaultVal=""
      if 'name' in port: name = port['name']
      if name:
        if '__type__' in port and  port['__type__']:
          typ = html.escape(port['__type__'])
        else:
          if 'type' in port: typ = html.escape(port['type'])

        if 'default' in port:
          defaultVal = port['default']
        else:
          defaultVal = port['defaultValue']

        if '__constraints__' in port and port['__constraints__']:
          constraints = port['__constraints__']
        else:
          if 'constraints' in port:
            constraints = port['constraints']

        if 'description' in port: description = port['description'].encode('raw-unicode-escape').decode('utf-8', 'ignore')

        res +='''
      <tr><td>$name </td><td>$type</td><td>$defaultVal</td>
       <td>$constraints</td><td>$description</td></tr>
        '''

        res=res.replace("$name", name)
        res=res.replace("$type", typ)
        res=res.replace("$defaultVal", str(defaultVal))
        res=res.replace("$constraints", constraints)
        res=res.replace("$description", description)

    res +="</tbody>\n  </table>"
  except:
    traceback.print_exc()
    pass
  return res


#########################################
#   Functions
#
def nvlist2dict(nvlist):
  res={}
  for v in nvlist:
    res[v.name] = v.value.value()
  return res

#
#
def dict2nvlist(dict) :
  import omniORB.any
  rslt = []
  for tmp in dict.keys() :
    rslt.append(SDOPackage.NameValue(tmp, omniORB.any.to_any(dict[tmp])))
  return rslt

#
#
def fmt_TimedString(data):
  res = "Time: %d\n" % (data.tm.sec + data.tm.nsec/10000000000.0)
  res += "Data: %s" % data.data
  return res

#
#
def print_configuration_set(conf_set, mark=""):
    print("-", conf_set.id, mark)
    for data in conf_set.configuration_data:
      print("  ",data.name, ":",data.value.value())

#
#
def start_cmd(arg):
    arg = "cmd /c start "+arg
    cmdline = arg.split()
    try:
      proc = subprocess.Popen(cmdline, shell=True)
      return proc
    except:
      traceback.print_exc()
    return None

#
#
def start_naming():
  if not check_process("omniNames") :
    subprocess.Popen(["cmd", "/c", "start", "rtm-naming.bat"])
    time.sleep(2)

########################################################
#   M A I N
def main():
  start_naming()

  if len(sys.argv) > 1:
    rtcmd=RtCmd(once=True)
    signal.signal(signal.SIGINT, lambda: rtcmd.close())
    rtcmd.onecmd(" ".join(sys.argv[1:]))
    rtcmd.close()
    print(rtcmd._error)
    return 
  else:
    RtCmd().cmdloop(intro="Welcome to RtCmd")
  return 


#########################################################################
#
#  M A I N 
#
if __name__=='__main__':
    main()
