from openai import OpenAI
from pydantic import BaseModel, Field
from typing import List, Optional
from dotenv import load_dotenv
from pprint import pprint
import os

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

class Beregningsresultat(BaseModel):
    """
    Model for Beregningsresultat list with name and value.
    """
    name: str = Field(description="Name of the Beregningsresultat")
    value: Optional[float] = Field(description="Quantity of Beregningsresultat")
    unit: Optional[str] = Field(description="Unit of measurement (e.g., kWh/år, liter/år, %)")
    
class Netto_energibudsjett(BaseModel):
    """
    Use this model when working with Energiattest.
    """
    title: str = Field(description="Netto energibudsjett")
    beregningsresultat: List[Beregningsresultat] = Field(description="List of Beregningsresultater needed for Netto energibudsjett")
 
def get_energibudsjett_from_text(energibudsjett_text: str) -> Netto_energibudsjett:
    """
    Convert Netto energibudsjett text into a structured Beregningsresultat object using OpenAI.
    """
    client = OpenAI()

    # Make the API call
    response = client.responses.parse(
        model="gpt-4o-mini-2024-07-18",
        input=[
            {"role": "user", "content": f"Convert this Energiattest into the specified format:\n\n{energibudsjett_text}"}
        ],
        text_format=Netto_energibudsjett
    )

    return response.output_parsed

    # Example usage
if __name__ == "__main__":
    # Read Energiattest text from file
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    energiattest_path = os.path.join(script_dir, "energiattest2.txt")
    with open(energiattest_path, "r") as file:
        energiattest_text = file.read()

    # Get structured recipe
    budsj = get_energibudsjett_from_text(energiattest_text)
    
    # Print results
    pprint(budsj)