# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 15:59:42 2023

8/10/23
checkpointed
changing to create image and OCR for teseract to better interpret headers which are tables

8/12/23

unrequiring subject for classification as email
terminating "in quotes" at "<" to allow for malfoemed quotes in email addresses

8/19/23
checkpoint
remove first head
finding first header by looking for any headers
finding 2d head by looking for repeat of whichver one come first

8/20/23
added code to replace blank page with text to avoid empty vector
checkpointed
changed match to terminate if nl is followed by !| or .
added * and - to the list

8/27/23
checkpoint
catch image conversion error and substitute dummy page

8/28/23
temp adding logging of each doc to catch hanging error

@author: tevslin


"""
import logging
import re
import dateparser
from typing import Iterator, List,  Optional, Union
from langchain.document_loaders.pdf import BasePDFLoader
#from langchain.document_loaders.unstructured import UnstructuredFileLoader
from langchain.document_loaders.blob_loaders import Blob
from langchain.docstore.document import Document


logger = logging.getLogger(__file__)

class EmailFromPDF(BasePDFLoader):
    """ Loads a PDF with pdf2imager and then scans with tesseract.
    Deliberately avoids considering multiple columns to better expose header data
    Attempts to find email fields in text and returns them in metadata if all mandatory fields
    exist.
    Optionally supports adding header to subsequent pages of a multipage email.
    Always returns page number in metadata.
    """

    def __init__(self,
        file_path: str,
         #if true, headers inserted at beginning of each page of multipage docs
        replicate_headers:Optional[bool]=False,
        #list of email heders to look for
        headers: Optional[dict]=
            {"from":["From:","FROM:"],
             "to":["To:","TO:"],
             "cc":["Cc:","CC:","cc:"],
             "subject":["Subject:","Re:","RE:","SUBJECT:"],
             "date":["Date:","Sent:","DATE:"],
             "inline-images":["Inline-Images:"],
             "attachments":["Attachments:"]},
        #headers which must be present in email. must be in order of expected appearance
        mandatory: Optional[List[str]]=["from","to"],
        #headers which return lists of strings
        lists: Optional[List[str]]=["to","cc","inline-images","attachments"],
        date_tag: Optional[str]="date", #local tag for date
        tesseract_location: Optional[str]= r'C:\Program Files\Tesseract-OCR\tesseract'
        #date_name: Optional[str]='date'
        #password: Optional[Union[str, bytes]] = None,
    ) -> None:
        """initialize with file name and password"""
        self.__saved_head__="" #saved header for optional replication
        self.__saved_metadata__={} #saved metadata for subsequent pages
        self.__saved_source__="" #saved name of last doc processed
        self.replicate_headers=replicate_headers
        self.headers=headers
        self.mandatory=mandatory
        self.lists=lists
        self.allheads=set().union(*self.headers.values())
        self.date_tag=date_tag
        self.tesseract_location=tesseract_location

        super().__init__(file_path)
        #self.parser=self.PDF2Image2Text

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
        #first_head_pos=email_text.find(self.first_head) #look for first item in mandatory
        first_head_pos=min((email_text.find(sub) for sub in self.allheads
                            if email_text.find(sub) != -1), default=None)
        if first_head_pos is None:   #if none found, not email
            return thedoc
        email_text=email_text[first_head_pos:] #drop all before
        first_head=email_text[:email_text.find(":")+1]
        second_head_pos=email_text.find(first_head,1) #look for repeat of first head
        if second_head_pos ==-1:
            docend=len(email_text)
        else:   #but if second header
            docend=second_head_pos+1 #stop the search there preserving one trailing character
        searchtext=email_text[:docend].replace('”','"').replace(': \n',':  ')
                                                                #special cleanup for header searching
        quote=False
        #getting rid of newlines inside quotes
        #will not be able to handle single quotes since they may be apostrophes
        st=""
        for char in searchtext:
            if char=='"':
                quote=not quote
            elif char=="<":
                quote=False #cures malformed quotes in email addreses
            elif char=="\n" and quote:
                char=""
            st+=char
        for key in self.headers:
            for header in self.headers[key]:
                
                match = re.search(f"{header}(.*?)(?=\n([a-zA-Z]|!|\||\.|\*|-))", st, re.DOTALL)
                #match = re.search(f"{header}(.*?)(?=\n(?! ))", st, re.DOTALL)
                if match:
                    lomatch=min(lomatch,match.span()[0])
                    himatch=max(himatch,match.span()[1])
                    field_content = match.group(1).strip().replace("\n", "")
                    fields[key] = field_content
                    if key in self.lists:
                        if ';' in fields[key]: #if there's a semicolon, probably it's the seperator
                            split=re.split(r';(?=(?:[^"]*"[^"]*")*[^"]*$)', fields[key])
                        elif fields[key].count(',')<=1: #if only one comma and no semicolons
                            split=[field_content,] #its not really a list
                        else:
                            split=re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', fields[key])
                            #split on commas
                        fields[key]=[field.strip() for field in split]
                    break        
            if (key in self.mandatory) and (key not in fields or len(fields[key])==0):
                #should get rid of malformed
                return thedoc

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
    
    def PDF2Image2Text(self,blob:Blob)->Iterator[Document]:
        """return the pages from the pdf as text derived from images"""
        from pdf2image import convert_from_path
        import pytesseract
        pytesseract.pytesseract.tesseract_cmd = self.tesseract_location
        logger.warning(blob.source)
        try:
            images = convert_from_path(blob.source) #get bit mapped images of pdf
        except Exception as e:
            logger.warning(f"{blob.source} had error {str(e)}. Dummy substituted.")
            images=["[doc couldn't be converted. this is a dummy page.]"]
        for page,image in enumerate(images):
            metadata = {"source": blob.source, "page": page}
            if isinstance(image,str):   #if already a string
                content=image #just use it
            else: #otherwise convert image
                content=pytesseract.image_to_string(image, config='--psm 6')
            yield Document(page_content=content,metadata=metadata)
            
    def load(self) -> List[Document]:
        """Load given path as pages."""
        return list(self.lazy_load())  

    def lazy_load(
        self,
    ) -> Iterator[Document]:
        """Lazy load given path as pages."""
        theList = list(self.PDF2Image2Text(Blob.from_path(self.file_path)))
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
            loader_kwargs={"replicate_headers":True},
            show_progress=True)
        documents=loader.load()
        for document in documents[:5]:
            print(document)
            