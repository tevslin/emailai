# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 15:59:42 2023

@author: tevslin


"""
import logging
import re
from typing import Iterator, List,  Optional, Union
import dateparser
from langchain.document_loaders import PyPDFLoader
from langchain.document_loaders.blob_loaders import Blob
from langchain.docstore.document import Document


logger = logging.getLogger(__file__)

class EmailFromPDF(PyPDFLoader):
    """ Loads a PDF with PyPDFLoader (which uses pypdf) and chunks at character level.
    Attempts to find email fields in text and returns them in metadata if all mandatory fields
    exist.
    Optionally supports adding header to subsequent pages of a multipage email.
    Always returns page number in metdata.
    """

    def __init__(self,
        file_path: str,
         #if true, headers inserted at beginning of each page of multipage docs
        replicate_headers:Optional[bool]=False,
        #list of email heders to look for
        headers: Optional[List[str]]=
            ["from","to","cc","subject","date","sent","inline-images","attachments"],
        #headers which must be present in email. must be in order of expected appearance
        mandatory: Optional[List[str]]=["from","to","subject"],
        #headers which return lists of strings
        lists: Optional[List[str]]=["to","cc","inline-images","attachments"],
        #dctionary of synonyms. value will be substituted for key in metadata
        synonyms: Optional[dict]={"sent":"date"},
        date_tag: Optional[str]="date", #local tag for date
        #date_name: Optional[str]='date'
        password: Optional[Union[str, bytes]] = None,
    ) -> None:
        """initialize with file name and password"""
        self.__saved_head__="" #saved header for optional replication
        self.__saved_metadata__={} #saved metadata for subsequent pages
        self.__saved_source__="" #saved name of last doc processed
        self.replicate_headers=replicate_headers
        self.headers=headers
        self.mandatory=mandatory
        self.lists=lists
        self.synonyms=synonyms
        self.date_tag=date_tag

        super().__init__(file_path,password=password)

    def parse_email(self,thedoc):
        """does the work of parsing pdf as possible email"""
        if thedoc.metadata['page']!=0: #if not first page
            if (thedoc.metadata['source']==self.__saved_source__) and \
                (self.mandatory[0] in self.__saved_metadata__):
                #if ff page of a known email
                thedoc.metadata.update(self.__saved_metadata__) #propagate the email metadata
                if self.replicate_headers: #if supposed to replicate headers
                    thedoc.page_content=self.__saved_head__+thedoc.page_content #do it
            return thedoc #exit for all but first page
        self.__saved_source__=thedoc.metadata['source'] #remember doc name
        self.__saved_head__='' #clear out saved head
        self.__saved_metadata__={} #and metadata
        fields = {}
        lomatch=99999 # we're going to get the span of he header to replicate in following pages
        himatch=0
        email_text=thedoc.page_content.replace(":\n",": ") #to help isolate header fields
        email_text_lc=email_text.lower()
        firsthead=email_text_lc.find(self.mandatory[0]) #look for first item in mandatory
        if firsthead==-1:   #if none found, not email
            return thedoc
        sechead=email_text_lc.find(self.mandatory[0],firsthead+1)
        if sechead==-1: #if no 2d from, OK to serach the whole doc
            docend=len(email_text)
        else:   #but if second header
            docend=sechead #stop the search there
        searchtext=email_text[:docend].replace('”','"') #special cleanup for header searching
        quote=False
        #getting rid of newlines inside quotes
        #will not be able to handle single quotes since they may be apostrophes
        st=""
        for char in searchtext:
            if char=='"':
                quote=not quote
            if char=="\n" and quote:
                char=""
            st+=char
        stl=st.lower()
        for header in self.headers:
            match = re.search(f"{header}:(.*?)(?=\n[a-zA-Z])", stl, re.DOTALL)
            if match:
                lomatch=min(lomatch,match.span()[0])
                himatch=max(himatch,match.span()[1])
                field_content = match.group(1).strip().replace("\n", "")
                fields[header] = field_content
                if header in self.lists:
                    if ';' in fields[header]: #if there's a semicolon, probably it's the seperator
                        split=re.split(r';(?=(?:[^"]*"[^"]*")*[^"]*$)', fields[header])
                    elif fields[header].count(',')<=1: #if only one comma and no semicolons
                        split=[field_content,] #its not really a list
                    else:
                        split=re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', fields[header])
                        #split on commas
                    fields[header]=[field.strip() for field in split]
            if (header in self.mandatory) and (header not in fields or len(fields[header])==0):
                #should get rid of malformed
                return thedoc
        for key, value in self.synonyms.items():    #replace synonyms
            if key in fields: #if taget found
                fields[value]=fields[key] #move the contents
                del fields[key]
        if self.date_tag in fields: #change date to unix timestamp at start of day
            try:    #don't want to die on malformed date
                date=dateparser.parse(fields[self.date_tag])
                date = date.replace(hour=0, minute=0, second=0, microsecond=0) #go back to midnight
                fields["udate"]=int(date.timestamp())
            except:
                logger.warning(f"malformed date not converted {fields[self.date_tag]} in {thedoc.metadata['source']}")
        thedoc.metadata.update(fields) #add any new metadata
        self.__saved_metadata__=fields #remember for propagation
        if self.replicate_headers:   #if replicating headers
            self.__saved_head__=st[lomatch:himatch] #remember them
        return thedoc
    def lazy_load(
        self,
    ) -> Iterator[Document]:
        """Lazy load given path as pages."""          
        theList = list(self.parser.parse(Blob.from_path(self.file_path)))
        for item in theList: #for each chunk returned
            yield self.parse_email(item)

if __name__ == '__main__':
    #test driver
    from langchain.document_loaders import DirectoryLoader
    while True:
        inputdir=input("what directory? (empty answer to terminate)")
        if len(inputdir)==0:
            break
        loader=DirectoryLoader(inputdir,"*.pdf",loader_cls=EmailFromPDF,
            loader_kwargs={"replicate_headers":True,"synonyms":{'attachments':'attached'}},
            show_progress=True)
        documents=loader.load()
        for document in documents[:5]:
            print(document)
            