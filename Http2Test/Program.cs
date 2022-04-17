using System.Net;

HttpClient myHttpClient = new HttpClient
    {
        DefaultRequestVersion = HttpVersion.Version20,
        DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower
    };

    string requestUrl = "https://http2.akamai.com/demo";

    try
    {
        Console.WriteLine($"GET {requestUrl}.");

        HttpResponseMessage response = await myHttpClient.GetAsync(requestUrl);
        response.EnsureSuccessStatusCode();

        Console.WriteLine($"Response HttpVersion: {response.Version}");

        string responseBody = await response.Content.ReadAsStringAsync();
        Console.WriteLine($"Response Body Length is: {responseBody.Length}");
        Console.WriteLine($"------------Response Body------------");
        Console.WriteLine(responseBody);
        Console.WriteLine($"------------End of Response Body------------");

    }
    catch (HttpRequestException e)
    {
        Console.WriteLine($"HttpRequestException : {e.Message}");
    }

    Console.WriteLine($"Press Enter to exit....");
    Console.ReadLine();
