import os
import json
import configparser
from typing import TypedDict, List, Dict, Union
from dotenv import load_dotenv
from pymongo import MongoClient
from confluent_kafka import Producer
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage
from langgraph.graph import StateGraph, END

# === Load environment variables ===
load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DATABASE")
MONGO_COLLECTION = os.getenv("MONGODB_COLLECTION")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
CC_TOPIC_NAME = os.getenv("CC_TOPIC_NAME")


# === Define the shared state ===
MovieRecState = Dict[str, Union[List[Dict], str]]


# === Kafka config loader ===
def load_kafka_config(path="client.properties"):
    config = configparser.ConfigParser()
    with open(path) as f:
        config.read_string("[default]\n" + f.read())
    return dict(config["default"])


# === Node: Load movies from MongoDB ===
def load_movies(state: MovieRecState) -> MovieRecState:
    print("\n\n📥 Step 1: Querying MongoDB for movies...")
    
    try:
        client = MongoClient(MONGO_URI)
        collection = client[MONGO_DB][MONGO_COLLECTION]
        
        query = {"imdb.rating": {"$exists": True, "$gte": 8.0}, "imdb.votes": {"$exists": True, "$gte": 50000}, "poster": {"$exists": True}}
        projection = {"title": 1, "plot": 1, "fullplot": 1, "genres": 1,"imdb.rating": 1, "year": 1,"_id": 0}
        movies = list(collection.find(query, projection).sort("imdb.rating", -1).limit(20))
        
        print(f"✅ Loaded {len(movies)} most popular movies by IMDB rating (with sufficient votes)")
        
        return {"movies": movies}
        
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return {"movies": []}



# === Node: Summarize with LangChain ===
def summarize_movies_node(state: MovieRecState) -> MovieRecState:
    print("\n\n🤖 Step 2: Summarizing movies with OpenAI...")
    
    movies = state.get("movies", [])
    if not movies:
        return {"summary": "No movies found to summarize."}
    
    movie_text = "\n".join([
        f"- \"{m['title']}\": {m.get('fullplot') or m.get('plot', 'No description.')}"
        for m in movies
    ])
    
    prompt = f"""
    You are a movie expert. Given the following list of movies from our database, create a summary with:

    1. A list of the 3 most interesting movies from this database result set
    2. Include at least the Title, Genre, and Brief Description, but feel free to get creative and include anything else you think would be interesting to note about a specific movie
    4. And if time was a constraint and you could recommend only 1 movie out of those 3, provide a movie title and reason why
    5. End with: "Powered by LangChain."

    IMPORTANT: Only include movies that are listed below. Do not add any movies from your general knowledge.

    Movies from our database:
    {movie_text[:8000]}
    """
    
    llm = ChatOpenAI(model=OPENAI_MODEL, temperature=0.7, api_key=OPENAI_API_KEY)
    response = llm.invoke([HumanMessage(content=prompt)])
    print("📝 Summary created by LLM:" + response.content + "...\n")

    return {"summary": response.content}


# === Node: Publish to Kafka ===
def publish_summary_node(state: MovieRecState) -> MovieRecState:
    print("\n\n📤 Step 3: Publishing to Kafka...")
    
    summary = state.get("summary", "No summary available")
    producer = Producer(load_kafka_config())
    
    def delivery_report(err, msg):
        if err:
            print(f"❌ Message delivery failed: {err}")
        else:
            print(f"✅ Message delivered to {msg.topic()}")
    
    producer.produce(CC_TOPIC_NAME, value=summary, callback=delivery_report)
    producer.flush()
    
    return {}


# === Graph Definition ===
def build_graph():
    workflow = StateGraph(MovieRecState)
    
    # Add nodes
    workflow.add_node("load_movies", load_movies)
    workflow.add_node("summarize_movies", summarize_movies_node)
    workflow.add_node("publish_summary", publish_summary_node)
    
    # Define flow
    workflow.add_edge("__start__", "load_movies")
    workflow.add_edge("load_movies", "summarize_movies")
    workflow.add_edge("summarize_movies", "publish_summary")
    workflow.add_edge("publish_summary", END)
    
    return workflow.compile()


# === Main ===
def main():
    print("🎬 Running LangChain Movie Recommender Summarizer workflow...\n")
    workflow = build_graph()
    initial_state: MovieRecState = {"movies": [], "summary": ""}
    workflow.invoke(initial_state)
    print("\n\n✅ LangChain Movie Recommender Demo Summarizer workflow complete!")


if __name__ == "__main__":
    main()
